defmodule OBRC do
  alias OBRC.{FileUtils, Worker, WorkerPool}

  def run([filename]), do: run(filename)

  def run([filename, store_impl]),
    do: run(filename, store_impl: parse_store_impl(store_impl))

  def run([filename, store_impl, n_workers]),
    do:
      run(filename,
        store_impl: parse_store_impl(store_impl),
        n_workers: parse_n_workers(n_workers)
      )

  defp parse_store_impl(s) when is_binary(s) do
    case String.split(s, "=") do
      [mod] ->
        {String.to_atom(mod), []}

      [mod, args] ->
        {String.to_atom(mod),
         String.split(args, ":")
         |> Enum.chunk_every(2)
         |> Enum.map(fn [failover, impl] ->
           {failover_sz, _} = Integer.parse(failover)
           {failover_sz, String.to_atom(impl)}
         end)}
    end
  end

  defp parse_n_workers(n) when is_binary(n) do
    {n, _} = Integer.parse(n)
    n
  end

  def run(filename, opts \\ []) do
    n_workers =
      case Keyword.fetch(opts, :n_workers) do
        :error -> System.schedulers_online()
        {:ok, n} when is_integer(n) -> n
      end

    {store_impl, store_impl_args} =
      case Keyword.fetch(opts, :store_impl) do
        :error -> {OBRC.Store.ETS, []}
        {:ok, {impl, args}} when is_atom(impl) and is_list(args) -> {impl, args}
      end

    FileUtils.break_file_into_blocks_of_lines!(filename)
    |> WorkerPool.process_in_parallel(
      fn request_work -> Worker.run(request_work, {store_impl, store_impl_args}) end,
      n: n_workers
    )
    |> merge_parallel_results()
    |> format_results()
    |> IO.puts()
  end

  defp merge_parallel_results(results) do
    results
    |> Enum.reduce(
      %{},
      &Map.merge(&1, &2, fn _station, {sum1, min1, max1, cnt1}, {sum2, min2, max2, cnt2} ->
        {sum1 + sum2, min(min1, min2), max(max1, max2), cnt1 + cnt2}
      end)
    )
  end

  defp format_results(results) do
    [
      "{",
      results
      |> Enum.sort_by(fn {station, _} -> station end)
      |> Enum.map(fn {station, {sum, min, max, cnt}} ->
        [
          station,
          "=",
          format_temp(min / 10),
          "/",
          format_temp(sum / cnt / 10),
          "/",
          format_temp(max / 10)
        ]
      end)
      |> Enum.intersperse(", "),
      "}"
    ]
  end

  defp format_temp(temp) do
    :erlang.float_to_binary(temp, decimals: 1)
  end
end

defmodule OBRC.FileUtils do
  @block_size 8 * 1024 * 1024

  def break_file_into_blocks_of_lines!(
        filename,
        block_size \\ @block_size,
        max_line_length \\ 1024
      )
      when is_binary(filename) and is_integer(block_size) and is_integer(max_line_length) do
    {:ok, %{size: file_size}} = File.stat(filename)
    {:ok, f} = :file.open(filename, [:binary])

    blocks = determine_break_points!(f, file_size, 0, block_size, max_line_length)

    :file.close(f)

    blocks
    |> Enum.map(fn {offset, length} -> {:block, filename, offset, length} end)
  end

  def read_block({:block, filename, offset, length}) do
    {:ok, f} = :prim_file.open(filename, [:binary, :raw, :read])
    {:ok, data} = :prim_file.pread(f, offset, length)
    :prim_file.close(f)
    data
  end

  defp determine_break_points!(f, file_size, offset, block_size, max_line_length) do
    remaining_bytes = file_size - offset

    if remaining_bytes <= block_size do
      [{offset, remaining_bytes}]
    else
      estimated_break_point = offset + block_size - Integer.floor_div(max_line_length, 2)

      {:ok, block} = :file.pread(f, estimated_break_point, max_line_length)

      case :binary.match(block, "\n") do
        {start, 1} ->
          # +1 because "\n"
          break_point = estimated_break_point + start + 1
          len = break_point - offset

          [
            {offset, len}
            | determine_break_points!(f, file_size, break_point, block_size, max_line_length)
          ]

        :nomatch ->
          raise "No line-end found within block"
      end
    end
  end
end

defmodule OBRC.WorkerPool do
  def process_in_parallel(blocks, worker_fn, opts \\ []) do
    {_pool, request_work, stop} = create(blocks)

    n =
      case Keyword.fetch(opts, :n) do
        :error -> System.schedulers_online()
        {:ok, n} -> n
      end

    IO.puts("Using #{n} workers")

    results =
      1..n
      |> Enum.map(fn _n ->
        Task.async(fn -> worker_fn.(request_work) end)
      end)
      |> Task.await_many(:infinity)

    stop.()

    results
  end

  defp create(blocks) do
    pool =
      spawn(fn ->
        loop(blocks)
      end)

    request_work = fn ->
      send(pool, {:req, self()})

      receive do
        nil -> nil
        data -> data
      end
    end

    stop = fn -> send(pool, :exit) end

    {pool, request_work, stop}
  end

  defp loop([]) do
    receive do
      {:req, p} ->
        send(p, nil)
        loop([])

      :exit ->
        nil
    end
  end

  defp loop([hd | tail]) do
    receive do
      {:req, p} ->
        send(p, hd)
        loop(tail)

      :exit ->
        tail
    end
  end
end

defmodule OBRC.Store do
  @compile {:inline, put: 3}
  def put({impl, state}, station, temp), do: {impl, apply(impl, :put, [state, station, temp])}

  def compact({impl, state}), do: {impl, apply(impl, :compact, [state])}

  def size({impl, state}), do: apply(impl, :size, [state])
  def collect({impl, state}), do: apply(impl, :collect, [state])
  def close({impl, state}), do: apply(impl, :close, [state])
end

defmodule OBRC.Store.ETS do
  def new([]) do
    table = :ets.new(:table, [:set, :private])
    {__MODULE__, table}
  end

  def put(table, station, temp) do
    case :ets.lookup(table, station) do
      [] ->
        :ets.insert_new(
          table,
          {:binary.copy(station), encode_sumcnt(temp, 1), encode_minmax(temp, temp)}
        )

      [{station, sumcnt, minmax}] ->
        mintemp = decode_min(minmax)
        maxtemp = decode_max(minmax)
        sumtemp = decode_sum(sumcnt)
        cnt = decode_cnt(sumcnt)

        new_sumcnt = encode_sumcnt(sumtemp + temp, cnt + 1)

        updates =
          if temp < mintemp or temp > maxtemp do
            [
              {2, new_sumcnt},
              {3, encode_minmax(min(mintemp, temp), max(maxtemp, temp))}
            ]
          else
            {2, new_sumcnt}
          end

        :ets.update_element(
          table,
          station,
          updates
        )
    end

    table
  end

  def compact(table), do: table

  def size(table) do
    :ets.info(table) |> Keyword.fetch!(:size)
  end

  def collect(table) do
    :ets.tab2list(table)
    |> Enum.map(fn {station, sumcnt, minmax} ->
      {station, {decode_sum(sumcnt), decode_min(minmax), decode_max(minmax), decode_cnt(sumcnt)}}
    end)
    |> Enum.into(%{})
  end

  def close(table) do
    :ets.delete(table)
    nil
  end

  @coldest_temp 100_0

  @compile {:inline, encode_minmax: 2}
  defp encode_minmax(min, max) do
    Bitwise.bor(
      Bitwise.bsl(max + @coldest_temp, 16),
      min + @coldest_temp
    )
  end

  @compile {:inline, encode_sumcnt: 2}
  defp encode_sumcnt(sum, cnt) do
    Bitwise.bor(
      Bitwise.bsl(cnt, 32),
      sum
    )
  end

  @compile {:inline, decode_sum: 1}
  defp decode_sum(sumcnt), do: Bitwise.band(sumcnt, 0xFFFF_FFFF)
  @compile {:inline, decode_cnt: 1}
  defp decode_cnt(sumcnt), do: Bitwise.bsr(sumcnt, 32)
  @compile {:inline, decode_min: 1}
  defp decode_min(minmax), do: Bitwise.band(minmax, 0xFFFF) - @coldest_temp
  @compile {:inline, decode_max: 1}
  defp decode_max(minmax), do: Bitwise.bsr(minmax, 16) - @coldest_temp
end

defmodule OBRC.Store.ETS.Unencoded do
  def new([]) do
    table = :ets.new(:table, [:set, :private])
    {__MODULE__, table}
  end

  def put(table, station, temp) do
    case :ets.lookup(table, station) do
      [] ->
        :ets.insert_new(table, {station, temp, temp, temp, 1})

      [{_key, sumtemp, mintemp, maxtemp, cnt}] ->
        :ets.insert(
          table,
          {station, sumtemp + temp, min(mintemp, temp), max(maxtemp, temp), cnt + 1}
        )
    end

    table
  end

  def compact(table), do: table

  def size(table) do
    :ets.info(table) |> Keyword.fetch!(:size)
  end

  def collect(table) do
    :ets.tab2list(table)
    |> Enum.map(fn {station, sum, min, max, cnt} ->
      {station, {sum, min, max, cnt}}
    end)
    |> Enum.into(%{})
  end

  def close(table) do
    :ets.delete(table)
    nil
  end
end

defmodule OBRC.Store.ProcessDict do
  def new([]) do
    state = 0
    {__MODULE__, state}
  end

  def put(state, station, temp) do
    case Process.get(station, nil) do
      nil ->
        Process.put(station, {temp, temp, temp, 1})
        state + 1

      {sumtemp, mintemp, maxtemp, cnt} ->
        Process.put(station, {sumtemp + temp, min(mintemp, temp), max(maxtemp, temp), cnt + 1})
        state
    end
  end

  def compact(state), do: state

  def size(state), do: state

  def collect(_state) do
    Process.get()
    |> Enum.flat_map(fn
      {station, {sum, min, max, cnt}} when is_binary(station) ->
        [{station, {sum, min, max, cnt}}]

      _ ->
        []
    end)
    |> Enum.into(%{})
  end

  def close(_state) do
    Process.get()
    |> Enum.each(fn
      {station, _} when is_binary(station) ->
        Process.delete(station)

      _ ->
        []
    end)

    nil
  end
end

defmodule OBRC.Store.Map do
  def new([]) do
    {__MODULE__, %{}}
  end

  def put(state, station, temp) do
    case Map.get(state, station) do
      nil ->
        state
        |> Map.put(station, {temp, temp, temp, 1})

      {sumtemp, mintemp, maxtemp, cnt} ->
        state
        |> Map.put(station, {sumtemp + temp, min(mintemp, temp), max(maxtemp, temp), cnt + 1})
    end
  end

  def compact(state), do: state

  def size(state), do: map_size(state)
  def collect(state), do: state
  def close(_state), do: nil
end

defmodule OBRC.Store.Merge do
  @merge_every 100

  def new([]) do
    state = {0, [], {:asc, []}}
    {__MODULE__, state}
  end

  def put({sz, entries, partitions} = _state, station, temp) do
    {sz + 1, [{station, temp} | entries], partitions}
    |> maybe_merge()
  end

  def compact(state) do
    {0, [], {sort, partitions}} = merge(state)

    {0, [],
     {sort,
      Enum.map(partitions, fn {station, sum, min, max, cnt} ->
        {copy_station(station), sum, min, max, cnt}
      end)}}
  end

  defp copy_station(station) do
    if :binary.referenced_byte_size(station) > byte_size(station) do
      :binary.copy(station)
    else
      station
    end
  end

  def size({sz, _, {_, partitions}}), do: sz + Enum.count(partitions)

  def collect(state) do
    {0, [], {_, partitions}} = merge(state)

    for {station, sum, min, max, cnt} <- partitions,
        into: %{},
        do: {station, {sum, min, max, cnt}}
  end

  def close(_state), do: nil

  defp maybe_merge({sz, _, _} = state) do
    if sz > @merge_every, do: merge(state), else: state
  end

  defp partition_entries(entries) do
    partitions =
      List.keysort(entries, 0, :desc)
      |> Enum.reduce([], fn
        {station, temp}, [{station, sum, min, max, cnt} | otherpartitions] ->
          [{station, sum + temp, min(min, temp), max(max, temp), cnt + 1} | otherpartitions]

        {station, temp}, partitions ->
          [{station, temp, temp, temp, 1} | partitions]
      end)

    {:asc, partitions}
  end

  defp merge_partitions({:asc, part1}, {:asc, part2}) do
    {:asc, merge_partitions(part1, part2, []) |> Enum.reverse()}
  end

  defp merge_partitions([], [], result) do
    result
  end

  defp merge_partitions([hd | tl], [], result) do
    merge_partitions(tl, [], [hd | result])
  end

  defp merge_partitions([], [hd | tl], result) do
    merge_partitions([], tl, [hd | result])
  end

  defp merge_partitions(
         [hd1 | tl1] = part1,
         [hd2 | tl2] = part2,
         result
       ) do
    {station1, sum1, min1, max1, cnt1} = hd1
    {station2, sum2, min2, max2, cnt2} = hd2

    cond do
      station1 < station2 ->
        merge_partitions(tl1, part2, [hd1 | result])

      station1 > station2 ->
        merge_partitions(part1, tl2, [hd2 | result])

      true ->
        merge_partitions(tl1, tl2, [
          {station1, sum1 + sum2, min(min1, min2), max(max1, max2), cnt1 + cnt2} | result
        ])
    end
  end

  defp merge({0, [], partitions}) do
    {0, [], partitions}
  end

  defp merge({_n, entries, partitions}) do
    {0, [], partition_entries(entries) |> merge_partitions(partitions)}
  end
end

defmodule OBRC.Store.MergeMap do
  @merge_every 120

  def new([]) do
    state = {0, [], %{}}
    {__MODULE__, state}
  end

  def put({sz, entries, map} = _state, station, temp) do
    {sz + 1, [{station, temp} | entries], map}
    |> maybe_merge()
  end

  def compact(state) do
    {0, [], map} = merge(state)

    new_map =
      for {station, value} <- map, into: %{}, do: {copy_station(station), accum_group(value)}

    {0, [], new_map}
  end

  defp copy_station(station) do
    if :binary.referenced_byte_size(station) > byte_size(station) do
      :binary.copy(station)
    else
      station
    end
  end

  def size({sz, _, map}), do: sz + map_size(map)

  def collect(state) do
    {0, [], map} = compact(state)
    map
  end

  def close(_state), do: nil

  defp maybe_merge({sz, _, _} = state) when sz > @merge_every, do: merge(state)
  defp maybe_merge({_sz, _, _} = state), do: state

  @compile {:inline, accum_group: 1}
  @compile {:inline, accum_groups: 2}

  defp accum_group(group) when is_list(group) do
    sum = Enum.sum(group)
    cnt = Enum.count(group)
    {min, max} = Enum.min_max(group)
    {sum, min, max, cnt}
  end

  defp accum_group(group) when is_tuple(group) do
    group
  end

  defp accum_groups(group1, group2) do
    {sum1, min1, max1, cnt1} = accum_group(group1)
    {sum2, min2, max2, cnt2} = accum_group(group2)
    {sum1 + sum2, min(min1, min2), max(max1, max2), cnt1 + cnt2}
  end

  defp merge_entries(entries, map) do
    groups = Enum.group_by(entries, &elem(&1, 0), &elem(&1, 1))

    Map.merge(map, groups, fn _key, left, right -> accum_groups(left, right) end)
  end

  defp merge({_n, entries, map}) do
    {0, [], merge_entries(entries, map)}
  end
end

defmodule OBRC.Store.MergeMap2 do
  @merge_every 100

  def new([]) do
    state = {%{}, %{}}
    {__MODULE__, state}
  end

  def put({entries, map} = _state, station, temp) do
    {put_map(entries, station, temp), map}
    |> maybe_merge()
  end

  defp put_map(map, station, temp) do
    case Map.get(map, station) do
      nil ->
        Map.put(map, station, {temp, temp, temp, 1})

      {sum, min, max, cnt} ->
        Map.put(map, station, {sum + temp, min(min, temp), max(max, temp), cnt + 1})
    end
  end

  def compact(state) do
    {%{}, map} = merge(state)
    new_map = for {station, value} <- map, into: %{}, do: {copy_station(station), value}
    {%{}, new_map}
  end

  defp copy_station(station) do
    if :binary.referenced_byte_size(station) > byte_size(station) do
      :binary.copy(station)
    else
      station
    end
  end

  def size({entries, map}), do: map_size(entries) + map_size(map)

  def collect(state) do
    {%{}, map} = merge(state)
    map
  end

  def close(_state), do: nil

  defp maybe_merge({entries, _} = state) when map_size(entries) > @merge_every, do: merge(state)
  defp maybe_merge({_, _} = state), do: state

  defp merge_entries(entries, map) do
    Map.merge(entries, map, fn _station, {sum1, min1, max1, cnt1}, {sum2, min2, max2, cnt2} ->
      {sum1 + sum2, min(min1, min2), max(max1, max2), cnt1 + cnt2}
    end)
  end

  defp merge({entries, map}) do
    {%{}, merge_entries(entries, map)}
  end
end

defmodule OBRC.Store.Adaptive do
  def new(args) do
    [{0, initial_impl} | failover] = args
    state = {initial_impl.new([]), [], failover}
    {__MODULE__, state}
  end

  def put(
        {st, st_tail, [{failover_sz, failover_impl} | failover_rest] = failover},
        station,
        temp
      ) do
    st = st |> OBRC.Store.put(station, temp)

    if OBRC.Store.size(st) > failover_sz do
      collected_st = OBRC.Store.collect(st)
      OBRC.Store.close(st)
      {apply(failover_impl, :new, [[]]), [collected_st | st_tail], failover_rest}
    else
      {st, st_tail, failover}
    end
  end

  def put({st, st_tail, []}, station, temp) do
    st = st |> OBRC.Store.put(station, temp)
    {st, st_tail, []}
  end

  def compact(state), do: state

  def size({st, st_tail, _}) do
    Enum.reduce(st_tail, 0, &(map_size(&1) + &2)) +
      OBRC.Store.size(st)
  end

  def collect({st, st_tail, _}) do
    st_tail
    |> Enum.reduce(
      OBRC.Store.collect(st),
      &merge/2
    )
  end

  defp merge(a, b) do
    Map.merge(a, b, fn _station, {sum1, min1, max1, cnt1}, {sum2, min2, max2, cnt2} ->
      {sum1 + sum2, min(min1, min2), max(max1, max2), cnt1 + cnt2}
    end)
  end

  def close({st, _st_tail, _}) do
    OBRC.Store.close(st)
  end
end

defmodule OBRC.Worker do
  def run(request_work, {store_impl, store_impl_args}) do
    store =
      apply(store_impl, :new, [store_impl_args])
      |> loop(request_work)

    map = OBRC.Store.collect(store)

    store |> OBRC.Store.close()

    map
  end

  defp loop(store, request_work) do
    case request_work.() do
      nil ->
        store

      block ->
        block
        |> OBRC.FileUtils.read_block()
        |> parse_lines(store)
        |> OBRC.Store.compact()
        |> loop(request_work)
    end
  end

  defp parse_lines(<<>>, store), do: store

  defp parse_lines(data, store) do
    {station, rest} = parse_station(data)
    {temp, rest} = parse_temp(rest)
    parse_lines(rest, store |> OBRC.Store.put(station, temp))
  end

  defp parse_station(data) do
    len = parse_station_length(data, 0)
    <<station::binary-size(len), ";", rest::binary>> = data
    {station, rest}
  end

  defp parse_station_length(<<";", _rest::binary>>, len), do: len
  defp parse_station_length(<<_ch, rest::binary>>, len), do: parse_station_length(rest, len + 1)

  defp parse_temp(data, temp \\ 0, sign \\ 1)
  defp parse_temp(<<"-", rest::binary>>, temp, sign), do: parse_temp(rest, temp, -1 * sign)
  defp parse_temp(<<".", rest::binary>>, temp, sign), do: parse_temp(rest, temp, sign)
  defp parse_temp(<<"\n", rest::binary>>, temp, sign), do: {sign * temp, rest}

  defp parse_temp(<<ch, rest::binary>>, temp, sign) when ch in ?0..?9 do
    parse_temp(rest, temp * 10 + (ch - ?0), sign)
  end
end

OBRC.run(System.argv())
