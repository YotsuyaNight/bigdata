defmodule BigData.DataNode do
  use GenServer
  require Logger

  # Clients

  def start_link(args) do
    GenServer.start_link(__MODULE__, nil, args)
  end

  def process_stream(pid, map, reduce, filename) do
    GenServer.call(pid, {:process_stream, map, reduce, filename}, :infinity)
  end

  # Server (callbacks)

  @impl true
  def init(_args) do
    Logger.info("Starting #{__MODULE__}")
    {:ok, nil}
  end

  @impl true
  def handle_call({:process_stream, map, reduce, filename}, _from, _state) do
    # n = :erlang.system_info(:logical_processors_available)
    n = 2
    self_pid = self()

    tasks =
      for i <- 0..(n - 1) do
        fn ->
          Logger.info("Task ##{i} started")

          partial_stream = chunk_stream(filename, i, n, 100)

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

          send(self_pid, {:result, result})
        end
      end

    partial_results = BigData.ParallelRunner.run(tasks)

    result =
      partial_results
      |> List.flatten()
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
