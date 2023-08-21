defmodule BigData.Cluster do
  use GenServer
  require Logger

  # Clients

  def start_link(args) do
    GenServer.start_link(__MODULE__, nil, args)
  end

  def map_reduce(pid, nodes, map, reduce, filename) do
    GenServer.call(pid, {:map_reduce, nodes, map, reduce, filename}, :infinity)
  end

  # Server (callbacks)

  @impl true
  def init(_args) do
    Logger.info("Starting #{__MODULE__}")
    {:ok, nil}
  end

  @impl true
  def handle_call({:map_reduce, nodes, map, reduce, filename}, _from, _state) do
    {_, task_supervisor} = Task.Supervisor.start_link(max_restarts: 2)
    self_pid = self()

    for {node, i} <- Enum.with_index(nodes) do
      Task.Supervisor.start_child(
        task_supervisor,
        fn ->
          Logger.info("Node ##{i} started")
          result = :rpc.call(node, :"Elixir.BigData.DataNode", :process_stream, [:data_node, map, reduce, filename], :infinity)
          case result do
            {:badrpc, error} -> raise BigData.Exception, message: "Entire cluster errored out", error: error
            _ -> send(self_pid, {:result, result})
          end
        end,
        restart: :transient
      )
    end

    task_list = for _ <- 1..length(nodes) do
      Logger.info("Awaiting receives")
      receive do {:result, result} -> result end
    end

    result = task_list |> Enum.flat_map(&(&1)) |> reduce.()

    {:reply, result, nil}
  end
end
