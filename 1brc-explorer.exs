Mix.install([{:explorer, "~> 0.8.0"}])

defmodule OBRC do
  alias OBRC.{FileUtils, Worker, WorkerPool}

  require Explorer.DataFrame

  def run([filename]) do
    FileUtils.break_file_into_blocks_of_lines!(filename)
    |> WorkerPool.process_in_parallel(&Worker.run/1)
    |> merge_parallel_results()
    |> format_results()
    |> IO.puts()
  end

  defp merge_parallel_results(results) do
    results
    |> Enum.filter(fn
      nil -> false
      _ -> true
    end)
    |> Explorer.DataFrame.concat_rows()
    |> Explorer.DataFrame.group_by(:station)
    |> Explorer.DataFrame.summarise(
      min: min(mintemp),
      max: max(maxtemp),
      mean: sum(sumtemp) / sum(cnttemp)
    )
    |> Explorer.DataFrame.collect()
    |> Explorer.DataFrame.to_rows(atom_keys: true)
  end

  defp format_results(results) do
    [
      "{",
      results
      |> Enum.map(fn %{station: station, min: min, max: max, mean: mean} ->
        [
          station,
          "=",
          format_temp(min),
          "/",
          format_temp(mean),
          "/",
          format_temp(max)
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
  @block_size 64 * 1024 * 1024

  #
  # Breaking file into blocks of lines
  #
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
end

defmodule OBRC.WorkerPool do
  def process_in_parallel(blocks, worker_fn) do
    {_pool, request_work, stop} = create(blocks)

    results =
      1..System.schedulers_online()
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

defmodule OBRC.Worker do
  require Explorer.DataFrame

  @load_csv_options [
    delimiter: ";",
    dtypes: [{:station, :string}, {:temp, {:f, 32}}],
    header: false,
    eol_delimiter: "\n",
    lazy: true,
    infer_schema_length: 1,
    parse_dates: false
  ]

  def run(request_work) do
    loop(request_work, nil)
  end

  defp loop(request_work, df) do
    case request_work.() do
      nil ->
        df

      block ->
        loop(request_work, process(block.(), df))
    end
  end

  defp process(block, nil) do
    Explorer.DataFrame.load_csv!(block, @load_csv_options)
    |> Explorer.DataFrame.group_by(:station)
    |> Explorer.DataFrame.summarise(
      mintemp: min(temp),
      maxtemp: max(temp),
      cnttemp: count(temp),
      sumtemp: sum(temp)
    )
  end

  defp process(block, df) do
    process(block, nil)
    |> Explorer.DataFrame.concat_rows(df)
    |> Explorer.DataFrame.group_by(:station)
    |> Explorer.DataFrame.summarise(
      mintemp: min(mintemp),
      maxtemp: max(maxtemp),
      cnttemp: sum(cnttemp),
      sumtemp: sum(sumtemp)
    )
  end
end

OBRC.run(System.argv())
