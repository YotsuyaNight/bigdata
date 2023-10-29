defmodule BigData.Examples.WordCount do
  def map(data) do
    data
    |> String.split()
    |> Enum.map(fn x -> {x, 1} end)
  end

  def reduce(data) do
    # data
    # |> Enum.group_by(&elem(&1, 0))
    # |> Enum.map(fn {k, v} -> {k, Enum.reduce(v, 0, fn x, acc -> acc + elem(x, 1) end)} end)
    data
    |> Enum.reduce(%{}, fn {k, v}, a -> Map.put(a, k, (a[k] || 0) + v) end)
    |> Enum.map(fn {k, v} -> {k, v} end)
  end
end
