defmodule BigData.Examples.Segmentation do
  def map(data) do
    line = String.split(data, ",")
    {age, _} = Integer.parse(Enum.at(line, 4))
    state = Enum.at(line, 3)
    age_group = div(age, 10) * 10
    is_male = if Enum.at(line, 5) == "Male", do: 1, else: 0
    is_female = if Enum.at(line, 5) == "Female", do: 1, else: 0
    [{"#{state};#{age_group}", {is_male, is_female}}]
  end

  def reduce(data) do
    data
    |> Enum.reduce(%{}, fn {k, v}, a ->
      current = a[k] || {0, 0}
      Map.put(a, k, {elem(current, 0) + elem(v, 0), elem(current, 1) + elem(v, 1)})
    end)
    |> Enum.map(fn {k, v} -> {k, v} end)
  end
end
