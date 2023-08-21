defmodule BigData.ParallelRunner do
  def run(tasks, options \\ [max_restarts: 2]) when is_list(tasks) and is_list(options) do
    {_, task_supervisor} = Task.Supervisor.start_link(max_restarts: options[:max_restarts] || 2)

    Enum.each(
      tasks,
      fn task ->
        Task.Supervisor.start_child(
          task_supervisor,
          task,
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
