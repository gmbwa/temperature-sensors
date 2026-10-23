defmodule TemperatureSensors.Writer do
  @copy "COPY temperature_readings (device_id, temperature_c, recorded_at) FROM STDIN WITH (FORMAT csv)"

  def save(batch) do
    if System.get_env("NO_COPY") == "1" do
      :ok
    else
      csv = Enum.map(batch, &csv_row/1)
      case Postgrex.transaction(TemperatureSensors.DB, fn conn ->
        csv
        |> Enum.into(Postgrex.stream(conn, @copy, []))
      end, timeout: 60_000) do
        {:ok, _} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp csv_row(r) do
    ts = DateTime.from_unix!(r.timestamp) |> DateTime.to_iso8601()
    escaped = String.replace(r.device_id, ~s("), ~s(""))
    [~s("#{escaped}"), ",", Float.to_string(r.temperature_c), ",", ts, "\n"]
    |> IO.iodata_to_binary()
  end
end
