defmodule BigData.ParallelRunner do
  @default_options [max_restarts: 1000, max_seconds: 1]

  def run(tasks, options \\ @default_options) when is_list(tasks) and is_list(options) do
    caller_pid = self()

    {_, task_supervisor} =
      Task.Supervisor.start_link(
        max_restarts: options[:max_restarts] || @default_options[:max_restarts],
        max_seconds: options[:max_seconds] || @default_options[:max_seconds]
      )

    Enum.each(
      tasks,
      fn task ->
        Task.Supervisor.start_child(
          task_supervisor,
          fn ->
            result = task.()
            send(caller_pid, {:result, result})
          end,
          restart: :transient
        )
      end
    )

    Enum.map(
      tasks,
      fn _ ->
        receive do
          {:result, result} -> result
        end
      end
    )
  end
end
