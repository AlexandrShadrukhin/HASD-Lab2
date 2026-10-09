package ru.hasd;

import org.apache.spark.sql.*;
import java.util.*;

public final class ValidationService {
    private ValidationService() {}
    public static void validate(Dataset<Row> data, long expected) {
        Map<String, String> types = Map.of("event_id", "string", "event_type", "string", "actor_id", "bigint",
            "actor_login", "string", "repo_id", "bigint", "repo_name", "string", "created_at", "timestamp",
            "event_date", "date", "event_hour", "int", "event_day_of_week", "int");
        if (data.schema().size() != types.size()) throw new IllegalStateException("Неожиданное число полей");
        for (var field : data.schema().fields()) if (!field.dataType().simpleString().equals(types.get(field.name()))) throw new IllegalStateException("Некорректный тип: " + field);
        long count = data.count();
        if (count == 0 || (expected >= 0 && count != expected)) throw new IllegalStateException("Не совпало число записей Parquet");
        if (data.filter("event_id IS NULL OR trim(event_id) = '' OR event_type IS NULL OR trim(event_type) = '' OR created_at IS NULL OR event_date IS NULL OR event_hour NOT BETWEEN 0 AND 23 OR event_day_of_week NOT BETWEEN 1 AND 7").limit(1).count() != 0) throw new IllegalStateException("Некорректные критические поля");
        if (data.select("event_id").distinct().count() != count) throw new IllegalStateException("Остались дубликаты event_id");
        System.out.println("Проверка Parquet пройдена: " + count + " событий");
    }
}
