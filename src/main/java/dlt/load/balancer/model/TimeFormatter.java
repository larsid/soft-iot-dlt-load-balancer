package dlt.load.balancer.model;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;

/**
 *
 * @author Uellington Damasceno
 */
public class TimeFormatter {

    public static String formatTimeElapsed(long timestampMillis) {
        if (timestampMillis <= 0) {
            return "data inválida";
        }

        Instant past = Instant.ofEpochMilli(timestampMillis);

        Instant now = Instant.now();

        if (past.isAfter(now)) {
            return "em breve";
        }

        LocalDateTime pastDateTime = LocalDateTime.ofInstant(past, ZoneId.systemDefault());
        LocalDateTime nowDateTime = LocalDateTime.ofInstant(now, ZoneId.systemDefault());

        long years = ChronoUnit.YEARS.between(pastDateTime, nowDateTime);
        if (years > 0) {
            return formatUnit(years, "ano", "anos");
        }

        long months = ChronoUnit.MONTHS.between(pastDateTime, nowDateTime);
        if (months > 0) {
            return formatUnit(months, "mês", "meses");
        }

        long days = ChronoUnit.DAYS.between(pastDateTime, nowDateTime);
        if (days > 0) {
            return formatUnit(days, "dia", "dias");
        }

        long hours = ChronoUnit.HOURS.between(pastDateTime, nowDateTime);
        if (hours > 0) {
            return formatUnit(hours, "hora", "horas");
        }

        long minutes = ChronoUnit.MINUTES.between(pastDateTime, nowDateTime);
        if (minutes > 0) {
            return formatUnit(minutes, "minuto", "minutos");
        }

        long seconds = ChronoUnit.SECONDS.between(pastDateTime, nowDateTime);
        if (seconds > 0) {
            return formatUnit(seconds, "segundo", "segundos");
        }

        return "agora mesmo";
    }

    private static String formatUnit(long value, String singular, String plural) {
        if (value == 1) {
            return "há " + value + " " + singular;
        }
        return "há " + value + " " + plural;
    }
}
