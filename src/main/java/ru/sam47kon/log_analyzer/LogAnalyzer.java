package ru.sam47kon.log_analyzer;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.math3.util.Pair;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.System.gc;

public class LogAnalyzer {
	public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("HH:mm:ss.SSS");
	public static final SimpleDateFormat TIME_LOG_FORMAT_FROM = new SimpleDateFormat("yyyy-MM-dd HH:mm");
	public static final SimpleDateFormat TIME_LOG_FORMAT_TO = new SimpleDateFormat("HH:mm");

	private static final String PATH_TO_LOG_FILE = "C:\\Users\\bulavin.ilya\\Downloads\\05\\SUP-1837769\\Logs_exp05_ws10_12_12_2025\\";
	private static final String SERVER_PATTERN = "server";
	private static final String PATTERN_IS_VERIFY = "Операция checkDocument";
	private static final boolean IS_05 = true;
	private static final String PATTERN_IS_TRANSITION = "[c.o.s.s.d.LifeCycleServiceImpl]";
	private static final String PATTERN_IS_TRANSITION_2 = "Переход ";
	private static final SimpleDateFormat TIME_LOG_FORMAT = new SimpleDateFormat("MM-dd;HH:mm:ss.SSS");
	private static final SimpleDateFormat TIME_LOG_FORMAT_05 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");

	private static final String SERVER_LOG_DETAILS = "serverLogDetails.log";

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
				try (Scanner scanner = new Scanner(file)) {
					logDebug("Анализ файла: " + file.getFileName());
					analyze(analysisData, infosByDate, scanner, IS_05 ? TIME_LOG_FORMAT_05 : TIME_LOG_FORMAT);
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

	public static void logDebug(String message) {
		System.out.println(DATE_FORMAT.format(new Date()) + ": " + message);
	}

	public static void logError(String errMessage) {
		System.err.println(DATE_FORMAT.format(new Date()) + ": " + errMessage);
	}

	private static void analyze(AnalysisData analysisData, TreeSet<Info> infosByDate, @NotNull Scanner scanner, SimpleDateFormat time_log_format) throws ParseException {
		boolean isFirstLine = true;
		String line = null;
		long numLine = 0;
		while (scanner.hasNextLine()) {
			line = scanner.nextLine();
			numLine++;
			if (numLine % 10000 == 0) {
				gc();
			}
			if (isFirstLine) {
				System.out.println("Начало лога: " + StringUtils.trim(StringUtils.substringBefore(line, " [")));
				isFirstLine = false;
			}
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

			Date timeLog = time_log_format.parse(StringUtils.substringBefore(line, " INFO"));
			String threadName = StringUtils.substringBetween(line, "[", "]");
			String log = StringUtils.substringAfter(line, PATTERN_IS_TRANSITION_2);
			String[] split = log.split(" для документа ");
			if (split.length != 2) {
				System.err.println("Ошибка парсинга");
				continue;
			}
			String transitionName = split[0];
			String guid = StringUtils.substringBetween(split[1], "[", "]");
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
		System.out.println("Конец лога: " + StringUtils.substringBefore(line, " ["));
	}

	private static @NotNull String startNotEndTransitions(@NotNull Map<String, List<Info>> analysis) {
		Map<String, List<Info>> startNotEndTransitions = new HashMap<>();
		analysis.forEach((guid, value) -> {
			value.sort(Comparator.comparing(Info::timeLog));
			List<Info> startTransitions = value.stream().filter(info -> info.isStart).toList();
			List<Info> endTransitions = value.stream().filter(info -> !info.isStart).collect(Collectors.toList());
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
						endTransitionsItr.remove();
						return;
					}
					break;
				}
				if (startNotEndTransitions.containsKey(guid)) {
					startNotEndTransitions.get(guid).add(startTransition);
				} else {
					startNotEndTransitions.put(guid, new ArrayList<>(Collections.singletonList(startTransition)));
				}
			});
		});
		if (startNotEndTransitions.isEmpty()) {
			return "0";
		}
		return startNotEndTransitions.size() + ":\n\t" + startNotEndTransitions.entrySet().stream()
				.sorted(Comparator.comparing(e -> e.getValue().get(0).timeLog))
				.map(e -> e.getKey() + ": " + e.getValue()).collect(Collectors.joining("\n\t"));
	}

	private static void logAnalysis(@NotNull AnalysisData analysisData, TreeSet<Info> infosByDate) {
		String fileName = PATH_TO_LOG_FILE + SERVER_LOG_DETAILS;
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
			SortedSet<TransitionCount> sortedTransitions = analysisData.transitionCounts.entrySet().stream().map(e -> new TransitionCount(e.getKey(), e.getValue())).collect(Collectors.toCollection(TreeSet::new));
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
			writeTransitionsByInterval(writer, infosByDate, 5);

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
				Info endInfo;
				if (transitionInfos.hasNext()) {
					// Если нет завершающего элемента для текущего старта, выходим из цикла
					endInfo = transitionInfos.next();
				} else {
					break;
				}
				// Вычисляем длительность перехода в миллисекундах
				long durationMillis = endInfo.timeLog().getTime() - startInfo.timeLog().getTime();
				// Проверяем, превышает ли длительность одну минуту (60 000 миллисекунд)
				if (durationMillis > 60_00) {
					// Пишем информацию о переходе
					longTransitions.add(new Pair<>(String.format("\n\t%s: %s %d ms", guid, startInfo, durationMillis), durationMillis));
				}
			}
		});
		writer.write(String.format("Переходы более 6 секунд: %d", longTransitions.size()));
		if (longTransitions.isEmpty()) {
			return;
		}
		longTransitions.sort((o1, o2) -> Long.compare(o2.getValue(), o1.getValue()));
		longTransitions.forEach(longTransition -> {
			try {
				writer.write(longTransition.getKey());
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		});
	}

	private static void writeTransitionsByInterval(BufferedWriter writer, @NotNull TreeSet<Info> infosByDate, int intervalMinutes) throws IOException {
		Map<String, Integer> transitionsByInterval = new TreeMap<>();

		for (Info info : infosByDate) {
			// Определяем начало интервала для текущей записи
			long intervalStart = (info.timeLog().getTime() / ((long) intervalMinutes * 60 * 1000)) * ((long) intervalMinutes * 60 * 1000);
			Date intervalStartDate = new Date(intervalStart);

			// Определяем конец интервала
			Date intervalEndDate = new Date(intervalStart + ((long) intervalMinutes * 60 * 1000));

			// Форматируем начало и конец интервала в строку
			String intervalKey = TIME_LOG_FORMAT_FROM.format(intervalStartDate) + "-" + TIME_LOG_FORMAT_TO.format(intervalEndDate);

			// Увеличиваем счетчик для этого интервала
			transitionsByInterval.put(intervalKey, transitionsByInterval.getOrDefault(intervalKey, 0) + 1);
		}

		// Записываем результат в файл
		writer.newLine();
		writer.write("Количество переходов по временным интервалам (" + intervalMinutes + " минут):");
		writer.newLine();
		for (Map.Entry<String, Integer> entry : transitionsByInterval.entrySet()) {
			writer.write(entry.getKey() + ": " + entry.getValue());
			writer.newLine();
		}
	}

	private static class AnalysisData {
		Map<String, List<Info>> analysis = new HashMap<>();
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
			int compareTransition = StringUtils.compare(data.transitionName, transitionName);
			return compareCount != 0 ? compareCount : compareTransition;
		}

		@Contract(pure = true)
		@Override
		public @NotNull String toString() {
			return transitionName + ": " + count;
		}
	}
}
