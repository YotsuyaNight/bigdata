defmodule BigData.Examples do
  def test_file, do: "C:\\dev\\test.txt"

  def small_file, do: "C:\\dev\\data.txt"

  def big_file, do: "C:\\dev\\data2.txt"

  def inline_data, do: "qwe asd zxc qwe asd zxc qwe asd zxc asd zxc asd zxc asd zxc zxc zxc zxc"

  def map(data) do
    data
      |> String.split
      |> Enum.map(fn x -> {x, 1} end)
  end

  def reduce(data) do
    data
      |> Enum.group_by(&(elem(&1, 0)))
      |> Enum.map(fn {k, v} -> {k, Enum.reduce(v, 0, fn (x, acc) -> acc + elem(x, 1) end)} end)
  end
end

# BigData.Supervisor.start_link
# f = File.stream!(BigData.Examples.big_file)
# BigData.measure(&BigData.DataNode.process_stream/4, [:master, &BigData.Examples.map/1, &BigData.Examples.reduce/1, f])
