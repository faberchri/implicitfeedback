package ch.uzh.ifi.vistatv.implicitfeedback;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.supercsv.cellprocessor.ParseDate;
import org.supercsv.cellprocessor.ParseDouble;
import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.ParseLong;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.exception.SuperCsvCellProcessorException;
import org.supercsv.io.CsvMapReader;
import org.supercsv.io.CsvMapWriter;
import org.supercsv.io.ICsvMapReader;
import org.supercsv.io.ICsvMapWriter;
import org.supercsv.prefs.CsvPreference;
import org.supercsv.util.CsvContext;

public class BBCRatingGenerator {

	private final static char usageDataDelimiter = '\t';

	private final static char epgDataDelimiter = '\t';

	private final static String epgKeyColumnHeader = "# epg:id";

	private final static String implicitRatingColumnHeader = "implicit_rating";

	private final static double START_TIME_TOLERANCE_PERCENTAGE = 5.0;

	private final static double END_TIME_TOLERANCE_PERCENTAGE = 5.0;

	private final static double p1 = 1.5;

	private final static double p2 = 1.5;

	private final static boolean writeHeaderInOutput = true;

	private final static String epgTimeZoneIdentifier = "GMT-1";

	private final Map<Object, Map<String, Object>> epg = new HashMap<>();

	private final CellProcessor[] epgProcessors = new CellProcessor[] {
			new NotNull(), // epg:id
			new ParseDateNullStringCheck("MM/dd/yy h:mm a", Locale.UK), // epg:start
			new ParseDateNullStringCheck("MM/dd/yy h:mm a", Locale.UK), // epg:end
			null, // channel:id
			null, // epg:title
			null, // epg:episode_title
			null, // epg:genres
	};

	private final CellProcessor[] usageProcessors = new CellProcessor[] {
			new NotNull(), // # user:id
			new ParseInt(), // aggregation:totalUserProgramDuration
			new ParseDouble(), // aggregation:totalUserProgramFraction
			new ParseLong(), // channel_session:start_time
			new ParseInt(), // channel_session:duration
			null, // channel:id
			null, // channel_session:id
			new ParseLong(), // program_session:start_time
			new ParseInt(), // program_session:duration
			new ParseInt(), // program_session:position
			null, // program_session:position_type
			new NotNull(), // epg:id
			new ParseInt() // epg:duration
	};

	private final String[] outHeader = new String[] { "# user:id",
			"aggregation:totalUserProgramDuration",
			"aggregation:totalUserProgramFraction",
			"channel_session:start_time", "channel_session:duration",
			"channel:id", "channel_session:id", "program_session:start_time",
			"program_session:duration", "program_session:position",
			"program_session:position_type", "epg:id", "epg:duration",
			implicitRatingColumnHeader };

	private final CellProcessor[] outProcessors = new CellProcessor[] {
			new NotNull(), // # user:id
			null, // aggregation:totalUserProgramDuration
			null, // aggregation:totalUserProgramFraction
			null, // channel_session:start_time
			null, // channel_session:duration
			null, // channel:id
			null, // channel_session:id
			null, // program_session:start_time
			null, // program_session:duration
			null, // program_session:position
			null, // program_session:position_type
			new NotNull(), // epg:id
			null, // epg:duration
			new NotNull() // implicit_rating
	};

	private final Logger log = Logger
			.getLogger(this.getClass().getSimpleName());

	/**
	 * Instantiates a new BBCRatingGenerator that uses the passed EPG data.
	 * 
	 * @param epgInp
	 *            the initial EPG data, can be null.
	 */
	public BBCRatingGenerator(Reader epgInp) {
		log.setLevel(Level.SEVERE);
		if (epgInp != null) {
			ICsvMapReader epgReader = new CsvMapReader(epgInp,
					new CsvPreference.Builder('"', epgDataDelimiter, "\n")
							.build());
			readEPGInMap(epgReader);
		}
	}

	/**
	 * Generates the implicit rating based on the stream of consumption
	 * information of a user and adds it to the specified writer.
	 * 
	 * @param usageInp
	 *            the consumption information
	 * @param epgInp
	 *            additional EPG information, can be null.
	 * @param out
	 *            the writer that captures the output
	 */
	public void generate(Reader usageInp, Reader epgInp, Writer out) {
		if (usageInp == null) {
			throw new IllegalArgumentException(
					"Usage input reader must not be null.");
		}

		CsvPreference usagePrefs = new CsvPreference.Builder('"',
				epgDataDelimiter, "\n").build();
		if (epgInp != null) {
			try (ICsvMapReader epgReader = new CsvMapReader(epgInp, usagePrefs)) {
				readEPGInMap(epgReader);
			} catch (IOException e) {
				log.severe("Error while closing EPG CSV reader.");
				e.printStackTrace();
			}
		}

		try (ICsvMapWriter writer = new CsvMapWriter(out, usagePrefs)) {
			if (writeHeaderInOutput) {
				writer.writeHeader(outHeader);
			}

			try (ICsvMapReader usageReader = new CsvMapReader(usageInp,
					new CsvPreference.Builder('"', usageDataDelimiter, "\n")
							.build());) {

				// the header columns are used as the keys to the tmpMap
				final String[] header = usageReader.getHeader(true);
				Map<String, Object> tmpMap;
				while ((tmpMap = usageReader.read(header, usageProcessors)) != null) {
					log.fine("Usage data read: "
							+ String.format(
									"lineNo=%s, rowNo=%s, customerMap=%s",
									usageReader.getLineNumber(),
									usageReader.getRowNumber(), tmpMap));
					double rating = getImplicitRating(tmpMap);
					write(writer, tmpMap, rating);
				}
			} catch (IOException e) {
				log.severe("Error while closing usage CSV reader.");
				e.printStackTrace();
			}
		} catch (IOException e) {
			log.severe("Error while closing csv output writer.");
			e.printStackTrace();
		}

	}

	/**
	 * Reads EPG data in the EPG map.
	 * 
	 * @param reader
	 *            the EPG CSV reader
	 */
	private void readEPGInMap(ICsvMapReader reader) {
		try {
			// the header columns are used as the keys to the tmpMap
			final String[] header = reader.getHeader(true);
			Map<String, Object> tmpMap;
			while ((tmpMap = reader.read(header, epgProcessors)) != null) {
				log.fine("EPG data read: "
						+ String.format("lineNo=%s, rowNo=%s, customerMap=%s",
								reader.getLineNumber(), reader.getRowNumber(),
								tmpMap));

				epg.put(tmpMap.get(epgKeyColumnHeader), tmpMap);
			}
		} catch (IOException e) {
			log.severe("Error while reading EPG data from CSV. Exiting!");
			e.printStackTrace();
			System.exit(-1);
		}

	}

	private void write(ICsvMapWriter writer, Map<String, Object> map,
			double rating) {
		log.info("Calculated rating for user "
				+ map.get("# user:id")
				+ " and epg-id "
				+ map.get("epg:id")
				+ ":\t"
				+ rating
				+ " (watched percentage: "
				+ ((Double) map.get("aggregation:totalUserProgramFraction"))
						.doubleValue() * 10 + ")");
		map.put(implicitRatingColumnHeader, rating);
		try {
			writer.write(map, outHeader, outProcessors);
		} catch (IOException e) {
			log.severe("Error while writing usage data to CSV. Exiting!");
			e.printStackTrace();
			System.exit(-1);
		}
	}

	private double getImplicitRating(Map<String, Object> entry) {

		// is in range 0-1 in data set
		double percentageWatched = ((Double) entry
				.get("aggregation:totalUserProgramFraction")).doubleValue() * 10;

		if (epg.get(entry.get("epg:id")) == null
				|| epg.get(entry.get("epg:id")).get("epg:start").equals("null")) {
			return percentageWatched;
		}

		boolean fromBeginning = watchedFromBeginningOfProgram(entry);
		boolean tillEnd = watchedTillEndOfProgram(entry);

		// printTimeLine(entry);

		if (fromBeginning && tillEnd) {
			log.info("User " + entry.get("# user:id")
					+ " for program with epg-id " + entry.get("epg:id")
					+ " watched from start till end.");
			return percentageWatched;
		}

		if (fromBeginning) {
			log.info("User " + entry.get("# user:id")
					+ " for program with epg-id " + entry.get("epg:id")
					+ " watched start only.");
			return Math.pow(percentageWatched, p1) / Math.pow(10.0, p1 - 1.0);
		}

		if (tillEnd) {
			log.info("User " + entry.get("# user:id")
					+ " for program with epg-id " + entry.get("epg:id")
					+ " watched end only.");
			return 10.0 - (Math.pow(10.0 - percentageWatched, p2) / Math.pow(
					10.0, p2 - 1.0));
		}
		log.info("User " + entry.get("# user:id") + " for program with epg-id "
				+ entry.get("epg:id") + " did not watch start or end.");
		return percentageWatched;
	}

	private boolean watchedFromBeginningOfProgram(Map<String, Object> entry) {

		Date epgStart = (Date) epg.get(entry.get("epg:id")).get("epg:start");

		long epgStartTime = epgStart.getTime() / 1000; // in seconds since start
														// of epoch

		long userStartTime = ((Long) entry.get("program_session:start_time"))
				.longValue(); // in seconds since start of epoch

		double tolerance = ((Integer) entry.get("epg:duration")).doubleValue(); // in
																				// seconds
		tolerance = (tolerance * START_TIME_TOLERANCE_PERCENTAGE) / 100.0;

		if (epgStartTime + tolerance >= userStartTime)
			return true;

		return false;
	}

	private boolean watchedTillEndOfProgram(Map<String, Object> entry) {

		Date epgEnd = (Date) epg.get(entry.get("epg:id")).get("epg:end");

		long epgEndTime = epgEnd.getTime() / 1000; // in seconds since start of
													// epoch

		long userEndTime = ((Long) entry.get("program_session:start_time"))
				.longValue(); // in seconds since start of epoch
		userEndTime = userEndTime
				+ ((Integer) entry.get("program_session:duration")).longValue(); // in
																					// seconds
		double tolerance = ((Integer) entry.get("epg:duration")).doubleValue(); // in
																				// seconds
		tolerance = (tolerance * END_TIME_TOLERANCE_PERCENTAGE) / 100.0;

		if (epgEndTime - tolerance <= userEndTime)
			return true;

		return false;
	}

	private void printTimeLine(Map<String, Object> entry) {

		Date epgStart = (Date) epg.get(entry.get("epg:id")).get("epg:start");

		Date epgEnd = (Date) epg.get(entry.get("epg:id")).get("epg:end");

		long userStartTime = ((Long) entry.get("program_session:start_time"))
				.longValue(); // in seconds since start of epoch

		Date userStart = new Date(userStartTime * 1000);

		long userEndTime = userStartTime
				+ ((Integer) entry.get("program_session:duration")).longValue();
		Date userEnd = new Date(userEndTime * 1000);

		System.out.println("EPG:\t" + epgStart + "\t" + epgEnd);
		System.out.println("User:\t" + userStart + "\t" + userEnd);

	}

	private class ParseDateNullStringCheck extends ParseDate {

		private final String dateFormat;
		private final Locale locale;

		public ParseDateNullStringCheck(final String format, Locale locale) {
			super(format);
			this.dateFormat = format;
			this.locale = locale;
		}

		@Override
		public Object execute(final Object value, final CsvContext context) {
			validateInputNotNull(value, context);

			if (!(value instanceof String)) {
				throw new SuperCsvCellProcessorException(String.class, value,
						context, this);
			}
			if (value.equals("null")) {
				return value;
			}
			try {
				final SimpleDateFormat formatter;
				if (locale == null) {
					formatter = new SimpleDateFormat(dateFormat);
				} else {
					formatter = new SimpleDateFormat(dateFormat, locale);
				}
				formatter.setLenient(false);
				formatter.setTimeZone(TimeZone
						.getTimeZone(epgTimeZoneIdentifier));
				final Date result = formatter.parse((String) value);
				return next.execute(result, context);
			} catch (final ParseException e) {
				throw new SuperCsvCellProcessorException(String.format(
						"'%s' could not be parsed as a Date", value), context,
						this, e);
			}
		}
	}
}
