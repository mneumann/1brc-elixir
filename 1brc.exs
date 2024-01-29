defmodule OBRC do
  def run() do
    [filename] = System.argv()
    run(filename)
  end

  def run(filename) do
    blocks = break_file_into_blocks_of_lines!(filename)

    {_pool, req_work, stop_pool} = create_pool(blocks)

    1..System.schedulers_online()
    |> Enum.map(fn _n ->
      Task.async(fn -> worker(req_work) end)
    end)
    |> Task.await_many(:infinity)
    |> Enum.reduce(%{}, fn a, b ->
      Map.merge(a, b, fn _place, f1, f2 -> accum_entry(f1, f2) end)
    end)
    |> format_results()
    |> IO.puts()

    stop_pool.()
  end

  defp create_pool(blocks) do
    pool =
      spawn(fn ->
        pool_loop(blocks)
      end)

    req_work = fn ->
      send(pool, {:req, self()})

      receive do
        nil -> nil
        data -> data
      end
    end

    stop = fn -> send(pool, :exit) end

    {pool, req_work, stop}
  end

  defp pool_loop([]) do
    receive do
      {:req, p} ->
        send(p, nil)
        pool_loop([])

      :exit ->
        nil
    end
  end

  defp pool_loop([hd | tail]) do
    receive do
      {:req, p} ->
        send(p, hd)
        pool_loop(tail)

      :exit ->
        tail
    end
  end

  defp worker(req_work) do
    # Create a local ETS table
    table = :ets.new(:table, [:set, :private])

    update_table = fn place, temp ->
      case :ets.lookup(table, place) do
        [] ->
          true = :ets.insert_new(table, {place, temp, temp, temp, 1})

        [{_key, sumtemp, mintemp, maxtemp, cnt}] ->
          :ets.insert(
            table,
            {place, sumtemp + temp, min(mintemp, temp), max(maxtemp, temp), cnt + 1}
          )
      end
    end

    worker_loop(req_work, update_table)

    # Convert ETS to Map
    map =
      :ets.tab2list(table)
      |> Enum.map(fn {place, sum, min, max, cnt} -> {place, {sum, min, max, cnt}} end)
      |> Enum.into(%{})

    :ets.delete(table)

    map
  end

  defp worker_loop(req_work, update_table) do
    case req_work.() do
      nil ->
        nil

      lazy_lines ->
        parse_lines(lazy_lines.(), update_table)
        worker_loop(req_work, update_table)
    end
  end

  defp format_results(results) do
    [
      "{",
      results
      |> Enum.map(fn {place, {sum, min, max, cnt}} ->
        [
          place,
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

  defp accum_entry({sum1, min1, max1, cnt1}, {sum2, min2, max2, cnt2}) do
    {sum1 + sum2, min(min1, min2), max(max1, max2), cnt1 + cnt2}
  end

  #
  # Breaking file into blocks of lines
  #

  @block_size 8 * 1024 * 1024

  defp break_file_into_blocks_of_lines!(
         filename,
         block_size \\ @block_size,
         max_line_length \\ 1024
       )
       when is_binary(filename) and is_integer(block_size) and is_integer(max_line_length) do
    {:ok, %{size: file_size}} = File.stat(filename)
    {:ok, f} = :file.open(filename, [:binary])

    blocks = determine_break_points!(f, file_size, 0, block_size, max_line_length)

    :file.close(f)

    # Return list of functions to lazy load each block of lines (within an actor)
    blocks
    |> Enum.map(fn {offset, length} ->
      fn ->
        {:ok, f} = :file.open(filename, [:binary])
        {:ok, data} = :file.pread(f, offset, length)
        :file.close(f)
        data
      end
    end)
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

  #
  # Parsing
  #

  defp parse_lines(<<>>, _cb), do: nil

  defp parse_lines(data, cb) do
    {station, rest} = parse_station(data)
    {temp, rest} = parse_temp(rest)
    cb.(station, temp)
    parse_lines(rest, cb)
  end

  defp parse_station(data) do
    len = parse_station_length(data, 0)
    <<station::binary-size(len), ?;, rest::binary>> = data
    {station, rest}
  end

  defp parse_station_length(<<?;, _rest::binary>>, len), do: len
  defp parse_station_length(<<_ch, rest::binary>>, len), do: parse_station_length(rest, len + 1)

  defp parse_temp(data, temp \\ 0, sign \\ 1)
  defp parse_temp(<<?-, rest::binary>>, temp, sign), do: parse_temp(rest, temp, -1 * sign)
  defp parse_temp(<<?., rest::binary>>, temp, sign), do: parse_temp(rest, temp, sign)
  defp parse_temp(<<?\n, rest::binary>>, temp, sign), do: {sign * temp, rest}

  defp parse_temp(<<ch, rest::binary>>, temp, sign) when ch in ?0..?9 do
    parse_temp(rest, temp * 10 + (ch - ?0), sign)
  end
end

OBRC.run()
