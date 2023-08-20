defmodule BigData.Cluster do
  use GenServer
  require Logger

  # Clients

  def start_link(name \\ nil) do
    GenServer.start_link(__MODULE__, nil, name: name)
  end

  def map_reduce(pid, nodes, map, reduce, filename) do
    GenServer.call(pid, {:map_reduce, nodes, map, reduce, filename}, :infinity)
  end

  # Server (callbacks)

  @impl true
  def init(_args) do
    {:ok, nil}
  end

  @impl true
  def handle_call({:map_reduce, nodes, map, reduce, filename}, _from, _state) do
    task_list =
      for {node, i} <- Enum.with_index(nodes) do
        Logger.info("Initializing Node ##{i}")

        task =
          Task.async(fn ->
            :rpc.call(node, :"Elixir.BigData.DataNode", :process_stream, [:master, map, reduce, filename], :infinity)
          end)

        Logger.info("Node ##{i} started")
        task
      end

    result =
      task_list
      |> Enum.map(&Task.await(&1, :infinity))
      |> Enum.flat_map(&catch_rpc_error/1)
      |> reduce.()

    {:reply, result, nil}
  rescue
    e in BigData.Exception -> {:reply, e, nil}
  end

  defp catch_rpc_error(result) do
    case result do
      {:badrpc, error} -> raise BigData.Exception, message: "An error occured when executing task", error: error
      _ -> result
    end
  end
end
