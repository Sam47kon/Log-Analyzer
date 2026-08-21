package ru.sam47kon.log_analyzer;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.math3.util.Pair;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LogAnalyzer {
	public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("HH:mm:ss.SSS");
	public static final SimpleDateFormat TIME_LOG_FORMAT_FROM = new SimpleDateFormat("yyyy-MM-dd HH:mm");
	public static final SimpleDateFormat TIME_LOG_FORMAT_TO = new SimpleDateFormat("HH:mm");
	// Можно менять под себя
	public static final boolean IS_NEW_DATE_FORMAT = true;
	public static final SimpleDateFormat TIME_LOG_FORMAT = IS_NEW_DATE_FORMAT
			? new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS")
			: new SimpleDateFormat("MM-dd;HH:mm:ss.SSS");

	private static final String PATH_TO_LOG_FILE = "C:\\Users\\bulavin.ilya\\Downloads\\SUP-2094701\\";
	// Можно менять под себя
	private static final int INTERVAL_MINUTES = 5;
	// Можно менять под себя
	private static final long LONG_TRANSITIONS = 6_000;

	private static final String SERVER_PATTERN = "server";
	private static final String PATTERN_IS_VERIFY = "Операция checkDocument";
	private static final String PATTERN_IS_TRANSITION = "c.o.s.s.d.LifeCycleServiceImpl]";
	private static final String PATTERN_IS_TRANSITION_2 = "Переход ";

	private static final String SERVER_LOG_DETAILS = "serverLogDetails.log";

	public static void logDebug(String message) {
		System.out.println(DATE_FORMAT.format(new Date()) + ": " + message);
	}

	public static void logError(String errMessage) {
		System.err.println(DATE_FORMAT.format(new Date()) + ": " + errMessage);
	}

	public static void main(String[] args) {
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		AnalysisData analysisData = new AnalysisData();
		TreeSet<Info> infosByDate = new TreeSet<>(Comparator.comparing(Info::timeLog));

		try (Stream<Path> paths = Files.find(
				Paths.get(PATH_TO_LOG_FILE),
				Integer.MAX_VALUE, // Максимальная глубина рекурсии (1 — только текущая папка, MAX_VALUE — рекурсивно)
				(path, attrs) -> attrs.isRegularFile() && path.getFileName().toString().startsWith(SERVER_PATTERN)
		)) {
			List<Path> matchingFiles = paths.collect(Collectors.toList());
			if (matchingFiles.isEmpty()) {
				logError(String.format("Нет файлов, соответствующих маске '%s'", SERVER_PATTERN));
				return;
			}
			matchingFiles.removeIf(file -> file.getFileName().toString().equals(SERVER_LOG_DETAILS));

			matchingFiles.forEach(file -> {
				try {
					// 1. Сначала определяем границы (начало и конец валидного лога)
					LogBounds bounds = findLogBounds(file);
					if (null == bounds) {
						logError("Файл не содержит корректных строк лога: " + file.getFileName());
						return;
					}
					analysisData.logBounds.add(bounds);

					// 2. Затем приступаем к анализу содержимого. Используем Scanner с явной кодировкой UTF-8
					try (Scanner scanner = new Scanner(file, StandardCharsets.UTF_8)) {
						analyze(analysisData, infosByDate, scanner);
					}
				} catch (Exception e) {
					logError(String.format("Ошибка при обработке файла [%s]: %s", file.getFileName(), ExceptionUtils.getRootCauseMessage(e)));
				}
			});
		} catch (IOException e) {
			logError(String.format("Ошибка при сканировании директории: %s", ExceptionUtils.getRootCauseMessage(e)));
		}

		stopWatch.stop();
		logDebug("Время анализа: " + stopWatch.getTime() + " ms");

		logAnalysis(analysisData, infosByDate);
	}

	/**
	 * Пробегает по файлу один раз, чтобы найти первую и последнюю строку,
	 * начинающуюся с корректной даты.
	 */
	@Nullable
	private static LogBounds findLogBounds(Path file) throws IOException {
		String firstValidLine = null;
		String lastValidLine = null;

		// Читаем построчно. Для очень больших файлов это может занять время,
		// но это необходимо, чтобы найти настоящий конец лога.
		try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
			String line;
			while ((line = reader.readLine()) != null) {
				if (isValidLogLine(line)) {
					if (firstValidLine == null) {
						firstValidLine = line;
					}
					lastValidLine = line;
				}
			}
		}

		if (firstValidLine != null) {
			return new LogBounds(file.getFileName().toString(),
					"Начало лога: " + StringUtils.trim(StringUtils.substringBefore(firstValidLine, " [")),
					"Конец лога: " + StringUtils.trim(StringUtils.substringBefore(lastValidLine, " [")));
		}
		return null;
	}

	/**
	 * Проверяет, начинается ли строка с даты в ожидаемом формате.
	 * Это защищает от строк вида "... 33 common frames omitted".
	 */
	private static boolean isValidLogLine(String line) {
		if (line == null || line.isEmpty()) {
			return false;
		}
		// Быстрая проверка: строка должна начинаться с цифры (года или месяца)
		char firstChar = line.charAt(0);
		if (!Character.isDigit(firstChar)) {
			return false;
		}

		try {
			// Пытаемся распарсить дату из начала строки.
			// Берем подстроку до первого пробела после секунд/миллисекунд или до " INFO/DEBUG"
			// Упрощенно: берем первые ~23 символа (yyyy-MM-dd HH:mm:ss,SSS)
			int len = Math.min(line.length(), 25);
			String datePart = line.substring(0, len);

			// Попытка парсинга. Если формат не совпадает, выбросит исключение.
			// Чтобы не создавать объекты Date каждый раз при проверке миллионов строк,
			// можно использовать более простую эвристику (регулярку), но parse надежнее.
			// Для оптимизации можно проверить наличие разделителей '-' и ':'
			if (!datePart.contains("-") || !datePart.contains(":")) {
				return false;
			}

			// Синхронизация не нужна, если используем локально в одном потоке,
			// но SimpleDateFormat не потокобезопасен. Здесь вызов в одном потоке чтения.
			TIME_LOG_FORMAT.parse(datePart.trim());
			return true;
		} catch (ParseException e) {
			return false;
		}
	}

	private static void analyze(AnalysisData analysisData, TreeSet<Info> infosByDate, @NotNull Scanner scanner) {
		String line;
		long numLine = 0;

		while (scanner.hasNextLine()) {
			line = scanner.nextLine();
			numLine++;

			// Оптимизация: gc() каждые 10000 строк может сильно тормозить.
			if (numLine % 100000 == 0) {
				System.gc(); // Лучше убрать, JVM сама справляется эффективнее
			}

			// Пропускаем строки, которые не являются началом записи лога (обрывки стектрейсов)
			if (!isValidLogLine(line)) {
				continue;
			}

			// Логика анализа
			if (line.contains(PATTERN_IS_VERIFY)) {
				if (line.contains("завершена")) {
					analysisData.endVerify++;
				} else {
					analysisData.startVerify++;
				}
				continue;
			}
			if (!line.contains(PATTERN_IS_TRANSITION) || !line.contains(PATTERN_IS_TRANSITION_2)) {
				continue;
			}

			// Парсинг даты (теперь мы уверены, что строка валидна)
			String dateStr = StringUtils.substringBefore(line, " INFO");
			// Если формат отличается (например DEBUG), нужно быть аккуратнее.
			// Предположим, что дата всегда в начале до первого слова уровня логирования.
			if (dateStr.length() > 25) {
				dateStr = dateStr.substring(0, 25);
			}

			Date timeLog;
			try {
				timeLog = TIME_LOG_FORMAT.parse(dateStr.trim());
			} catch (ParseException e) {
				// Если вдруг строка прошла проверку isValidLogLine, но здесь ошибка - пропускаем
				continue;
			}

			String threadName = StringUtils.substringBetween(line, "[", "]");
			if (threadName == null) {
				continue; // Защита от null
			}

			String log = StringUtils.substringAfter(line, PATTERN_IS_TRANSITION_2);
			String[] split = log.split(" для документа ");
			if (split.length != 2) {
				System.err.println("Ошибка парсинга строки: " + line);
				continue;
			}
			String transitionName = split[0];
			String guid = StringUtils.substringBetween(split[1], "[", "]");
			if (guid == null) {
				continue;
			}

			boolean isStart = "запущен.".equals(StringUtils.substringAfter(split[1], "] "));
			if (isStart) {
				analysisData.startTransitionsCount++;
				analysisData.transitionCounts.compute(transitionName, (k, v) -> v == null ? 1 : v + 1);
			} else {
				analysisData.endTransitionsCount++;
			}

			Info info = new Info(timeLog, transitionName, threadName, isStart);
			if (isStart) {
				infosByDate.add(info);
			}
			if (analysisData.analysis.containsKey(guid)) {
				analysisData.analysis.get(guid).add(info);
			} else {
				List<Info> infos = new ArrayList<>();
				infos.add(info);
				analysisData.analysis.put(guid, infos);
			}
		}
	}

	private static @NotNull String startNotEndTransitions(@NotNull Map<String, List<Info>> analysis) {
		Map<String, List<Info>> startNotEndTransitions = new HashMap<>();
		analysis.forEach((guid, value) -> {
			value.sort(Comparator.comparing(Info::timeLog));
			List<Info> startTransitions = value.stream().filter(Info::isStart).toList();
			List<Info> endTransitions = value.stream().filter(info -> !info.isStart()).collect(Collectors.toList());

			startTransitions.forEach(startTransition -> {
				Iterator<Info> endTransitionsItr = endTransitions.iterator();
				while (endTransitionsItr.hasNext()) {
					Info endTransition = endTransitionsItr.next();
					if (!startTransition.transitionName.equals(endTransition.transitionName)) {
						if (!endTransitionsItr.hasNext()) {
							break;
						}
						continue;
					}
					if (startTransition.timeLog.after(endTransition.timeLog)) {
						continue;
					}
					if (startTransition.timeLog.before(endTransition.timeLog) || startTransition.timeLog.equals(endTransition.timeLog)) {
						// Нашли пару
						endTransitionsItr.remove();
						return;
					}
					break;
				}
				startNotEndTransitions.computeIfAbsent(guid, k -> new ArrayList<>()).add(startTransition);
			});
		});
		if (startNotEndTransitions.isEmpty()) {
			return "0";
		}
		return startNotEndTransitions.size() + ":\n\t" + startNotEndTransitions.entrySet().stream()
				.sorted(Comparator.comparing(e -> e.getValue().get(0).timeLog()))
				.map(e -> e.getKey() + ": " + e.getValue())
				.collect(Collectors.joining("\n\t"));
	}

	private static void logAnalysis(@NotNull AnalysisData analysisData, TreeSet<Info> infosByDate) {
		String fileName = PATH_TO_LOG_FILE + SERVER_LOG_DETAILS;
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName, StandardCharsets.UTF_8))) {
			SortedSet<TransitionCount> sortedTransitions = analysisData.transitionCounts.entrySet().stream()
					.map(e -> new TransitionCount(e.getKey(), e.getValue()))
					.collect(Collectors.toCollection(TreeSet::new));
			for (LogBounds bound : analysisData.logBounds) {
				writer.write(bound.toString());
				writer.newLine();
			}
			writer.newLine();

			writer.write("Запущено переходов: " + analysisData.startTransitionsCount);
			writer.newLine();
			writer.write("Завершено переходов: " + analysisData.endTransitionsCount);
			writer.newLine();
			writer.write("Запущенные и не завершенные переходы: " + startNotEndTransitions(analysisData.analysis));
			writer.newLine();
			writer.write("Запущено проверок: " + analysisData.startVerify);
			writer.newLine();
			writer.write("Завершено проверок: " + analysisData.endVerify);
			writer.newLine();
			writer.write("Переходы:\n\t" + sortedTransitions.stream().map(Record::toString).collect(Collectors.joining("\n\t")));
			writer.newLine();

			List<String> list = analysisData.analysis.entrySet().stream()
					.filter(entry -> entry.getValue().size() / 2 >= 10)
					.map(entry -> entry.getKey() + ": [" + entry.getValue().size() / 2 + "] " +
							entry.getValue().stream()
									.map(info -> info.transitionName)
									.distinct()
									.collect(Collectors.joining(",")))
					.toList();

			writer.write("Более 10 переходов у документов: " + list.size() + System.lineSeparator() + StringUtils.join(list, System.lineSeparator()));
			if (!list.isEmpty()) {
				writer.newLine();
			}

			writeLongestTransitions(writer, analysisData.analysis);

			// Интервал в минутах
			writeTransitionsByInterval(writer, infosByDate, INTERVAL_MINUTES);

			logDebug("Данные успешно записаны в файл: " + fileName);
		} catch (IOException e) {
			logError("Ошибка при записи в файл: " + e.getMessage());
		}
	}

	private static void writeLongestTransitions(@NotNull BufferedWriter writer, @NotNull Map<String, List<Info>> analysis) throws IOException {
		List<Pair<String, Long>> longTransitions = new ArrayList<>();
		analysis.forEach((guid, transitions) -> {
			Iterator<Info> transitionInfos = transitions.iterator();
			while (transitionInfos.hasNext()) {
				Info startInfo = transitionInfos.next();
				if (!transitionInfos.hasNext()) {
					// Если нет завершающего элемента для текущего старта, выходим из цикла
					break;
				}
				Info endInfo = transitionInfos.next();

				// Вычисляем длительность перехода в миллисекундах
				long durationMillis = endInfo.timeLog().getTime() - startInfo.timeLog().getTime();
				// Проверяем, превышает ли длительность N ms
				if (durationMillis > LONG_TRANSITIONS) {
					// Пишем информацию о переходе
					longTransitions.add(new Pair<>(String.format("\n\t%s: %s %d ms", guid, startInfo, durationMillis), durationMillis));
				}
			}
		});
		writer.write(String.format("Переходы более %d секунд: %d", LONG_TRANSITIONS / 1000, longTransitions.size()));
		if (longTransitions.isEmpty()) {
			return;
		}
		longTransitions.sort((o1, o2) -> Long.compare(o2.getValue(), o1.getValue()));
		for (Pair<String, Long> lt : longTransitions) {
			writer.write(lt.getKey());
		}
	}

	private static void writeTransitionsByInterval(BufferedWriter writer, @NotNull TreeSet<Info> infosByDate, @SuppressWarnings("SameParameterValue") int intervalMinutes) throws IOException {
		Map<String, Integer> transitionsByInterval = new TreeMap<>();
		long intervalMillis = (long) intervalMinutes * 60 * 1000;

		for (Info info : infosByDate) {
			// Определяем начало интервала для текущей записи
			long intervalStart = (info.timeLog().getTime() / intervalMillis) * intervalMillis;
			Date intervalStartDate = new Date(intervalStart);

			// Определяем конец интервала
			Date intervalEndDate = new Date(intervalStart + intervalMillis);

			// Форматируем начало и конец интервала в строку
			String intervalKey = TIME_LOG_FORMAT_FROM.format(intervalStartDate) + "-" + TIME_LOG_FORMAT_TO.format(intervalEndDate);

			// Увеличиваем счетчик для этого интервала
			transitionsByInterval.put(intervalKey, transitionsByInterval.getOrDefault(intervalKey, 0) + 1);
		}

		// Записываем результат в файл
		writer.newLine();
		writer.write("Количество переходов по временным интервалам (" + intervalMinutes + " минут):");
		writer.newLine();

		if (!transitionsByInterval.isEmpty()) {
			List<String> lines = transitionsByInterval.entrySet().stream()
					.map(entry -> entry.getKey() + ": " + entry.getValue())
					.toList();
			writer.write(String.join(System.lineSeparator(), lines));
		}
	}

	// Класс-контейнер для границ лога
	private static class LogBounds {
		String fileName;
		String startLine;
		String endLine;

		LogBounds(String fileName, String start, String end) {
			this.fileName = fileName;
			this.startLine = start;
			this.endLine = end;
		}

		@Override
		public String toString() {
			return fileName + ":" + "\n" + startLine + "\n" + endLine;
		}
	}

	private static class AnalysisData {
		Map<String, List<Info>> analysis = new HashMap<>();
		List<LogBounds> logBounds = new ArrayList<>();
		Map<String, Integer> transitionCounts = new HashMap<>();
		int startVerify = 0;
		int endVerify = 0;
		int startTransitionsCount = 0;
		int endTransitionsCount = 0;
	}

	private record Info(Date timeLog, String transitionName, String threadName, boolean isStart) {
		@Contract(pure = true)
		@Override
		public @NotNull String toString() {
			return DATE_FORMAT.format(timeLog) + ": " + transitionName + ", " + threadName;
		}
	}

	record TransitionCount(String transitionName, int count) implements Comparable<TransitionCount> {
		@Override
		public int compareTo(@NotNull TransitionCount data) {
			int compareCount = Integer.compare(data.count, count);
			return compareCount != 0 ? compareCount : StringUtils.compare(data.transitionName, transitionName);
		}

		@Contract(pure = true)
		@Override
		public @NotNull String toString() {
			return transitionName + ": " + count;
		}
	}
}