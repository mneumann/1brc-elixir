defmodule OBRC do
  alias OBRC.{FileUtils, Worker, WorkerPool}

  def run([filename]), do: run(filename)

  def run([filename, store_impl]),
    do: run(filename, store_impl: store_impl)

  def run([filename, store_impl, n_workers]),
    do: run(filename, store_impl: store_impl, n_workers: n_workers)

  def run(filename, opts \\ []) do
    n_workers =
      case Keyword.fetch(opts, :n_workers) do
        :error -> System.schedulers_online()
        {:ok, n} when is_integer(n) -> n
        {:ok, n} when is_binary(n) -> elem(Integer.parse(n), 0)
      end

    store_impl =
      case Keyword.fetch(opts, :store_impl) do
        :error -> OBRC.Store.ETS
        {:ok, impl} when is_atom(impl) -> impl
        {:ok, impl} when is_binary(impl) -> String.to_atom(impl)
      end

    FileUtils.break_file_into_blocks_of_lines!(filename)
    |> WorkerPool.process_in_parallel(fn request_work -> Worker.run(request_work, store_impl) end,
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
  def collect_into({impl, state}, into), do: apply(impl, :collect_into, [state, into])
  def close({impl, state}), do: apply(impl, :close, [state])
end

defmodule OBRC.Store.ETS do
  def new() do
    table = :ets.new(:table, [:set, :private])
    {__MODULE__, table}
  end

  def put(table, station, temp) do
    case :ets.lookup(table, station) do
      [] ->
        true =
          :ets.insert_new(table, {station, encode_sumcnt(temp, 1), encode_minmax(temp, temp)})

      [{_key, sumcnt, minmax}] ->
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

  def collect_into(table, into) do
    :ets.tab2list(table)
    |> Enum.map(fn {station, sumcnt, minmax} ->
      {station, {decode_sum(sumcnt), decode_min(minmax), decode_max(minmax), decode_cnt(sumcnt)}}
    end)
    |> Enum.into(into)
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
  def new() do
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

  def collect_into(table, into) do
    :ets.tab2list(table)
    |> Enum.map(fn {station, sum, min, max, cnt} ->
      {station, {sum, min, max, cnt}}
    end)
    |> Enum.into(into)
  end

  def close(table) do
    :ets.delete(table)
    nil
  end
end

defmodule OBRC.Store.ProcessDict do
  def new() do
    state = 0
    {__MODULE__, state}
  end

  def put(state, station, temp) do
    case :erlang.get(station) do
      :undefined ->
        :erlang.put(station, {temp, temp, temp, 1})
        state + 1

      {sumtemp, mintemp, maxtemp, cnt} ->
        :erlang.put(station, {sumtemp + temp, min(mintemp, temp), max(maxtemp, temp), cnt + 1})
        state
    end
  end

  def collect_into(_state, into) do
    :erlang.get()
    |> Enum.flat_map(fn
      {station, {sum, min, max, cnt}} when is_binary(station) ->
        [{station, {sum, min, max, cnt}}]

      _ ->
        []
    end)
    |> Enum.into(into)
  end

  def close(_state) do
    nil
  end
end

defmodule OBRC.Worker do
  def run(request_work, store_impl) do
    store =
      apply(store_impl, :new, [])
      |> loop(request_work)

    map = OBRC.Store.collect_into(store, %{})

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
