defmodule BigData do
  use Application
  @type keyval :: {String | Symbol, any}
  @type mapper :: (any -> [keyval])
  @type reducer :: ([keyval] -> [keyval])

  @impl true
  def start(_type, _args) do
    BigData.Supervisor.start_link
    {:ok, self()}
  end

  def measure(func, args) do
    IO.puts("Measuring function runtime")
    {time, result} = :timer.tc(func, args)
    us = rem(time, 1000)
    ms = rem(div(time, 1000), 1000)
    s = div(time, 1000 * 1000)
    IO.puts("Runtime was: #{s}s #{ms}ms #{us}us")
    result
  end

  def test(nodes, filename) do
    BigData.measure(&BigData.Cluster.map_reduce/5, [:cluster, nodes, &BigData.Examples.map/1, &BigData.Examples.reduce/1, filename])
  end
end
