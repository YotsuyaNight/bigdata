defmodule BigData.DataNode do
  use GenServer
  require Logger

  # Clients

  def start_link(name \\ nil) do
    GenServer.start_link(__MODULE__, nil, name: name)
  end

  def process_stream(pid, map, reduce, filename) do
    GenServer.call(pid, {:process_stream, map, reduce, filename}, :infinity)
  end

  # Server (callbacks)

  @impl true
  def init(_args) do
    {:ok, nil}
  end

  @impl true
  def handle_call({:process_stream, map, reduce, filename}, _from, _state) do
    n = :erlang.system_info(:logical_processors_available)
    # n = 4

    task_list =
      for i <- 0..(n - 1) do
        partial_stream = chunk_stream(filename, i, n, 100)

        task =
          Task.async(fn ->
            result =
              partial_stream
              |> Stream.flat_map(fn chunk ->
                {_, worker} = BigData.Worker.start_link()
                # Logger.info("Spawned a worker #{inspect(worker)} for Task #{i}")
                result = BigData.Worker.map_reduce(worker, map, reduce, chunk)
                BigData.Worker.stop(worker)
                result
              end)
              |> reduce.()

            Logger.info("Task ##{i} finished")
            result
          end)

        Logger.info("Task ##{i} started")
        task
      end

    result =
      task_list
      |> Enum.flat_map(&Task.await(&1, :infinity))
      |> reduce.()

    {:reply, result, nil}
  end

  defp chunk_stream(filename, i, n, size) do
    File.stream!(filename)
    |> Stream.drop(i)
    |> Stream.take_every(n)
    |> Stream.chunk_every(size)
  end
end
