defmodule BigData.Supervisor do
  use Supervisor

  def start_link do
    Supervisor.start_link(__MODULE__, :ok, [])
  end

  @impl true
  def init(:ok) do
    children = [
      %{
        id: :cluster,
        start: {BigData.Cluster, :start_link, [:cluster]}
      },
      %{
        id: :master,
        start: {BigData.DataNode, :start_link, [:master]}
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
