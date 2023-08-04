defmodule BigData.Examples do
  def data do
    "qwe asd zxc qwe asd zxc qwe asd zxc asd zxc asd zxc asd zxc asd zxc zxc zxc zxc zxc zxc"
  end

  def map(data) do
    data
      |> String.split
      |> Enum.map(fn x -> {x, 1} end)
  end

  def reduce(data) do
    data
      |> Enum.group_by(fn x -> elem(x, 0) end)
      |> Enum.map(fn {k, v} -> {k, Enum.count(v)} end)
  end
end
