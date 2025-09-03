defmodule CreateMeasurements do
  defp stations() do
    File.stream!("data/weather_stations.csv")
    |> Stream.reject(&String.starts_with?(&1, "#"))
    |> Stream.map(fn line ->
      [station, temp] = line |> String.trim() |> String.split(";")
      {temp, ""} = Float.parse(temp)
      {station, temp}
    end)
    |> Stream.with_index()
    |> Stream.map(fn {data, index} -> {index, data} end)
    |> Enum.into(%{})
  end

  defp gen_random(n, stations) when is_map(stations) do
    size = map_size(stations)

    for _ <- 1..n do
      index = :rand.uniform(size) - 1
      {station, temp} = stations |> Map.get(index)
      delta = :rand.uniform(21) - 11
      temp = (temp |> trunc) + delta
      fract = :rand.uniform(10) - 1
      "#{station};#{temp}.#{fract}\n"
    end
  end

  @bulk 10000

  defp write_to_file(_path, 0, _stations), do: nil

  defp write_to_file(path, count, stations) do
    n = min(count, @bulk)
    data = gen_random(n, stations)
    File.write!(path, data, [:append, :create])
    write_to_file(path, count - n, stations)
  end

  def run([count]) do
    {count, ""} = count |> Integer.parse()
    path = "measurements.#{count}.txt"
    File.write!(path, "")
    write_to_file(path, count, stations())
  end
end

CreateMeasurements.run(System.argv())
