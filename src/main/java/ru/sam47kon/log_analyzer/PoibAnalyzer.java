package ru.sam47kon.log_analyzer;

import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.time.StopWatch;
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
import static ru.sam47kon.log_analyzer.LogAnalyzer.*;

public class PoibAnalyzer {

	private static final String PATH_TO_LOG_FILE = "C:\\Users\\bulavin.ilya\\Downloads\\fr07_logs\\";
	private static final int INTERVAL_MINUTES = 5;

	private static final String POIB_PATTERN = "poib";
	private static final String REQUEST_SOBI_PATTERN = "Сформирован запрос на ";
	private static final SimpleDateFormat TIME_LOG_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
	private static final String POIB_LOG_DETAILS = "poibLogDetails.log";

	public static void main(String[] args) {
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();

		Map<String, List<Info>> analysisData = new HashMap<>();
		try (Stream<Path> paths = Files.find(
				Paths.get(PATH_TO_LOG_FILE),
				Integer.MAX_VALUE, // Максимальная глубина рекурсии (1 — только текущая папка, MAX_VALUE — рекурсивно)
				(path, attrs) -> attrs.isRegularFile() && path.getFileName().toString().startsWith(POIB_PATTERN)
		)) {
			List<Path> matchingFiles = paths.collect(Collectors.toList());
			if (matchingFiles.isEmpty()) {
				logError(String.format("Нет файлов, соответствующих маске '%s'", POIB_PATTERN));
				return;
			}
			matchingFiles.removeIf(file -> file.getFileName().toString().equals(POIB_LOG_DETAILS));

			matchingFiles.forEach(file -> {
				try (Scanner scanner = new Scanner(file)) {
					logDebug("Анализ файла: " + file.getFileName());
					analyze(scanner, analysisData);
				} catch (Exception e) {
					logError(String.format("Ошибка при обработке файла [%s]: %s", file.getFileName(), ExceptionUtils.getRootCauseMessage(e)));
				}
			});
		} catch (
				IOException e) {
			logError(String.format("Ошибка при сканировании директории: %s", ExceptionUtils.getRootCauseMessage(e)));
		}

		writeAnalysis(analysisData);
		stopWatch.stop();
		logDebug("Время анализа: " + stopWatch.getTime() + " ms");
	}

	private static void analyze(@NotNull Scanner scanner, Map<String, List<Info>> analysisData) throws ParseException {
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
			if (!line.contains(REQUEST_SOBI_PATTERN)) {
				continue;
			}
			Date timeLog = TIME_LOG_FORMAT.parse(StringUtils.substringBefore(line, " DEBUG"));
			String typeRequest = StringUtils.substringAfter(StringUtils.substringBetween(line, REQUEST_SOBI_PATTERN, "("), ": ");
			String threadName = StringUtils.substringBetween(line, "[", "]");
			String hash = StringUtils.substringBetween(line, "(", ")");
			String request = getRequest(typeRequest, line);
			Info info = new Info(timeLog, typeRequest, threadName, hash, request);

			if (analysisData.containsKey(typeRequest)) {
				analysisData.get(typeRequest).add(info);
			} else {
				analysisData.put(typeRequest, new ArrayList<>(Collections.singletonList(info)));
			}

		}
		System.out.println("Конец лога: " + StringUtils.substringBefore(line, " ["));
	}

	private static @NotNull String getRequest(@NotNull String typeRequest, String line) {
		String request = StringUtils.substringAfter(line, typeRequest);
		if (!typeRequest.equals("getAllowedResources")) {
			return "";
		}
		return "количество ресурсов: " + StringUtils.countMatches(request, "SobiResourceActionPair");
	}

	private static void writeAnalysis(@NotNull Map<String, List<Info>> analysisData) {
		String fileName = PATH_TO_LOG_FILE + POIB_LOG_DETAILS;
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
			writer.write(String.format("Запросы SOBI: \n\t%s", analysisData.entrySet().stream().map(e -> e.getKey() + ": " + e.getValue().size()).collect(Collectors.joining("\n\t"))));
			writer.newLine();
			writer.write("Количество запросов по временным интервалам (" + INTERVAL_MINUTES + " минут):");
			analysisData.forEach((typeRequest, requestData) -> writeRequestsByInterval(typeRequest, writer, requestData));
		} catch (IOException e) {
			logError("Ошибка при записи в файл: " + e.getMessage());
		}
		logDebug("Данные успешно записаны в файл: " + fileName);
	}

	@SneakyThrows
	private static void writeRequestsByInterval(String typeRequest, @NotNull BufferedWriter writer, @NotNull List<Info> requestData) {
		requestData.sort(Comparator.comparing(o -> o.timeLog));
		Map<String, Integer> requestsByInterval = new TreeMap<>();
		for (Info info : requestData) {
			// Определяем начало интервала для текущей записи
			long intervalStart = (info.timeLog().getTime() / ((long) INTERVAL_MINUTES * 60 * 1000)) * ((long) INTERVAL_MINUTES * 60 * 1000);
			Date intervalStartDate = new Date(intervalStart);

			// Определяем конец интервала
			Date intervalEndDate = new Date(intervalStart + ((long) INTERVAL_MINUTES * 60 * 1000));

			// Форматируем начало и конец интервала в строку
			String intervalKey = TIME_LOG_FORMAT_FROM.format(intervalStartDate) + "-" + TIME_LOG_FORMAT_TO.format(intervalEndDate);

			// Увеличиваем счетчик для этого интервала
			requestsByInterval.put(intervalKey, requestsByInterval.getOrDefault(intervalKey, 0) + 1);
		}

		// Записываем результат в файл
		writer.newLine();
		writer.write(String.format("%s:\n", typeRequest));
		for (Map.Entry<String, Integer> entry : requestsByInterval.entrySet()) {
			writer.write(entry.getKey() + ": " + entry.getValue());
			writer.newLine();
		}
	}

	private record Info(Date timeLog, String typeRequest, String threadName, String hash, String request) {
		@Contract(pure = true)
		@Override
		public @NotNull String toString() {
			return DATE_FORMAT.format(timeLog) + ": " + hash + " " + typeRequest + ", [" + threadName + "]";
		}
	}
}
