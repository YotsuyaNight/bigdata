defmodule BigData do
  use Application
  require Logger

  @type keyval :: {String | Symbol, any}
  @type mapper :: (any -> [keyval])
  @type reducer :: ([keyval] -> [keyval])

  @impl true
  def start(_type, _args) do
    Logger.info("Starting application on node: #{Node.self()}")
    BigData.AppSupervisor.start_link()
    {:ok, self()}
  end

  def measure(func, args) do
    Logger.info("Measuring function runtime")
    {time, result} = :timer.tc(func, args)
    us = rem(time, 1000)
    ms = rem(div(time, 1000), 1000)
    s = div(time, 1000 * 1000)
    Logger.info("Runtime was: #{s}s #{ms}ms #{us}us")
    result
  end

  def test(filename, nodes) do
    BigData.measure(
      &BigData.Cluster.map_reduce/5,
      [
        :cluster,
        nodes || [:bigdata@master, :bigdata@node1, :bigdata@node2, :bigdata@node3],
        &BigData.Examples.Segmentation.map/1,
        &BigData.Examples.Segmentation.reduce/1,
        filename
      ]
    )
  end
end
