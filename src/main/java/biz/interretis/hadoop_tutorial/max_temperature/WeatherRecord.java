package biz.interretis.hadoop_tutorial.max_temperature;

public enum WeatherRecord {

    YEAR(15, 4),
    AIR_TEMPERATURE(87, 5),
    QUALITY(92, 1);

    public static final int MISSING = 9999;

    private final int start;
    private final int length;

    private WeatherRecord(final int start, final int length) {
        this.start = start;
        this.length = length;
    }

    public String stringValue(final String line) {
        return line.substring(start, start + length);
    }

    public int integerValue(final String line) {
        int digitsBegin;
        if (line.charAt(start) == '+') {
            digitsBegin = start + 1;
        } else {
            digitsBegin = start;
        }
        final String number = line.substring(digitsBegin, start + length);
        return Integer.parseInt(number);
    }
}
