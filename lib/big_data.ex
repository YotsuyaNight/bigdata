defmodule BigData do
  @type keyval :: {String | Symbol, any}
  @type mapper :: (any -> [keyval])
  @type reducer :: ([keyval] -> [keyval])

  def measure(func, args) do
    IO.puts("Measuring function runtime")
    {time, result} = :timer.tc(func, args)
    us = rem(time, 1000)
    ms = rem(div(time, 1000), 1000)
    s = div(time, 1000 * 1000)
    IO.puts("Runtime was: #{s}s #{ms}ms #{us}us")
    result
  end
end
