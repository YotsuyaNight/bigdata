defmodule BigData.Crash do
  @filepath "/tmp/crash_counter.txt"
  @crash_freq 2

  def attempt do
    value = :rand.uniform(@crash_freq)

    if value == 1 do
      raise BigData.Exception, message: "Scheduled test crash occured"
    end
  end
end
