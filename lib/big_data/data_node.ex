defmodule BigData.DataNode do
  use GenServer

  # Clients

  def start_link(name \\ nil) do
    GenServer.start_link(__MODULE__, nil, name: name)
  end

  def process_stream(pid, map, reduce, stream) do
    GenServer.call(pid, {:process_stream, map, reduce, stream}, :infinity)
  end

  # Server (callbacks)

  @impl true
  def init(_args) do
    {:ok, nil}
  end

  @impl true
  def handle_call({:process_stream, map, reduce, stream}, _from, _state) do
    n = :erlang.system_info(:logical_processors_available)

    task_list =
      for i <- 0..(n - 1) do
        partial_stream = chunk_stream(stream, i, n, 1000)
        Task.async(fn ->
          {_, worker} = BigData.Worker.start_link()
          IO.puts("Spawned a worker #{inspect(worker)}")
          result = BigData.Worker.map_reduce(worker, map, reduce, partial_stream)
          BigData.Worker.stop(worker)
          result
        end)
      end

    result =
      task_list
      |> Enum.flat_map(&Task.await(&1, :infinity))
      |> reduce.()

    {:reply, result, nil}
  end

  defp chunk_stream(stream, i, n, size) do
    stream
    |> Stream.drop(i)
    |> Stream.take_every(n)
    |> Stream.chunk_every(size)
  end
end
