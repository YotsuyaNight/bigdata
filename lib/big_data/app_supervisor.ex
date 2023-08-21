defmodule BigData.AppSupervisor do
  use Supervisor

  def start_link do
    Supervisor.start_link(__MODULE__, :ok, [])
  end

  @impl true
  def init(:ok) do
    children = [
      {BigData.Cluster, name: :cluster},
      {BigData.DataNode, name: :data_node}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
