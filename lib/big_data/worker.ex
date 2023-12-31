defmodule BigData.Worker do
  use GenServer

  # Clients

  def start_link(name \\ nil) do
    GenServer.start_link(__MODULE__, nil, name: name)
  end

  def stop(pid) do
    GenServer.cast(pid, {:stop})
  end

  def map_reduce(pid, map, reduce, stream) do
    GenServer.call(pid, {:map_reduce, map, reduce, stream}, :infinity)
  end

  # Server (callbacks)

  @impl true
  def init(_args) do
    {:ok, nil}
  end

  @impl true
  @spec handle_call({:map_reduce, BigData.mapper(), BigData.reducer(), Stream}, any, any) ::
          {:reply, [BigData.keyval()], any}
  def handle_call({:map_reduce, map, reduce, chunk}, _from, _state) do
    BigData.Crash.attempt()

    result =
      chunk
      |> Enum.flat_map(&map.(&1))
      |> reduce.()

    {:reply, result, nil}
  end

  @impl true
  def handle_cast({:stop}, _state) do
    {:stop, :normal, nil}
  end
end
