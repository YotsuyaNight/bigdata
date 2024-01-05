defmodule BigData.SupervisedRunner do
  @default_options [max_restarts: 1_000_000, max_seconds: 1]

  def spawn_supervisor(options \\ @default_options) when is_list(options) do
    {_, task_supervisor} =
      Task.Supervisor.start_link(
        max_restarts: options[:max_restarts] || @default_options[:max_restarts],
        max_seconds: options[:max_seconds] || @default_options[:max_seconds]
      )

    task_supervisor
  end

  def run(task_supervisor, task) do
    caller_pid = self()

    Task.Supervisor.start_child(
      task_supervisor,
      fn ->
        result = task.()
        send(caller_pid, {:result, result})
      end,
      restart: :transient
    )

    receive do
      {:result, result} -> result
    end
  end
end
