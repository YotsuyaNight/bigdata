defmodule BigData.DataNode do
  use GenServer

  @type mapper :: (any -> [BigData.keyval])
  @type reducer :: ([BigData.keyval] -> [BigData.keyval])

  # Clients

  def start_link(name \\ nil) do
    GenServer.start_link(__MODULE__, nil, [name: name])
  end

  @spec map(pid, mapper, any) :: [BigData.keyval]
  def map(pid, map, data) do
    GenServer.call(pid, {:map, map, data})
  end

  @spec reduce(pid, reducer, any) :: [BigData.keyval]
  def reduce(pid, reduce, data) do
    GenServer.call(pid, {:reduce, reduce, data})
  end

  @spec process(pid, (any -> list), (list -> list), any) :: list
  def process(pid, map, reduce, data) do
    reduce(pid, reduce, map(pid, map, data))
  end

  # Server (callbacks)

  @impl true
  def init(_args) do
    {:ok, nil}
  end

  @impl true
  @spec handle_call({:map, mapper, any}, any, any) :: {:reply, [BigData.keyval], any}
  def handle_call({:map, map, data}, _from, _state) do
    result = map.(data)
    {:reply, result, nil}
  end

  @impl true
  @spec handle_call({:reduce, reducer, any}, any, any) :: {:reply, [BigData.keyval], any}
  def handle_call({:reduce, reduce, data}, _from, _state) do
    result = reduce.(data)
    {:reply, result, nil}
  end
end
