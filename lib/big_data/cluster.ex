defmodule BigData.Cluster do
  use GenServer

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
        IO.puts("Initializing Node ##{i}")

        task =
          Task.async(fn ->
            :rpc.call(node, :"Elixir.BigData.DataNode", :process_stream, [:master, map, reduce, filename], :infinity)
          end)

        IO.puts("Node ##{i} started")
        task
      end

    result =
      task_list
      |> Enum.flat_map(&Task.await(&1, :infinity))
      |> reduce.()

    {:reply, result, nil}
  end
end
