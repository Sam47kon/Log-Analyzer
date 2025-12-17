package ru.sam47kon.log_analyzer;

import com.opencsv.CSVWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import ru.sam47kon.log_analyzer.LogAnalyzer.TransitionCount;

import java.io.FileOutputStream;
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

import static ru.sam47kon.log_analyzer.LogAnalyzer.logDebug;
import static ru.sam47kon.log_analyzer.LogAnalyzer.logError;

public class DLCPerfAnalyzer {

	private static final String DLC_PERF_PATTERN = "dlcperf";
	private static final String PATH_TO_LOG_FILE = "C:\\Users\\bulavin.ilya\\Downloads\\05\\SUP-1837769\\Logs_exp05_ws10_12_12_2025\\";
	private static final String DELIMITER = ": ";
	private static final String PATTERN_INFO = "Детали перехода ";
	private static final String MS = "ms";
	private static final String SEPARATOR = System.lineSeparator() + "\t";
	private static final String DETAIL_LOG_CSV = "detailLog.csv";
	private static final String DETAIL_LOG_XLSX = "detailLog.xlsx";
	private static final SimpleDateFormat TIME_LOG_FORMAT_05 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
	private static final String DEBUG = " DEBUG";

	// 2025-03-27 09:24:17,376 DEBUG [ExecutorService424b17d1-3dc5-4497-8c72-a3bf790ce2609] [LIFECYCLE-PERF-LOG] []: Детали перехода 9c9dd388-641f-4966-b96c-583a4da08074 для документа MSC_ApplCashFlowShrt:

	public static void main(String[] args) {
		List<Info> infos = new ArrayList<>();
		Date startLog = new Date(java.sql.Date.valueOf("2050-01-01").getTime());
		Date endLog = new Date(java.sql.Date.valueOf("2000-01-01").getTime());
		try (Stream<Path> paths = Files.find(
				Paths.get(PATH_TO_LOG_FILE),
				Integer.MAX_VALUE, // Максимальная глубина рекурсии (1 — только текущая папка, MAX_VALUE — рекурсивно)
				(path, attrs) -> attrs.isRegularFile() && path.getFileName().toString().startsWith(DLC_PERF_PATTERN)
		)) {
			List<Path> matchingFiles = paths.toList();
			if (matchingFiles.isEmpty()) {
				logError(String.format("Нет файлов, соответствующих маске '%s'", DLC_PERF_PATTERN));
				return;
			}
			matchingFiles.forEach(file -> {
				try (Scanner scanner = new Scanner(file.toFile())) {
					logDebug("Анализ файла: " + file.getFileName());
					analyze(scanner, infos, startLog, endLog);
				} catch (Exception e) {
					logError(String.format("Ошибка при обработке файла [%s]: %s", file.getFileName(), ExceptionUtils.getRootCauseMessage(e)));
				}
			});
		} catch (IOException e) {
			logError(String.format("Ошибка при сканировании директории: %s", ExceptionUtils.getRootCauseMessage(e)));
		}

		infos.sort(Comparator.naturalOrder());
		System.out.println("Начало лога: " + TIME_LOG_FORMAT_05.format(startLog));
		System.out.println("Конец лога: " + TIME_LOG_FORMAT_05.format(endLog));
		System.out.println("Всего успешных переходов: " + infos.size());
		// "Переходы:
		System.out.println(formatTransitionCounts1(infos));
		List<Info> longInfos = infos.stream().filter(info -> info.time > 15000).toList();
		System.out.printf("Более 15 секунд: %d%n\t%s%n", longInfos.size(), StringUtils.join(longInfos.size() > 100 ? longInfos.subList(0, 100) : longInfos, SEPARATOR));

		writeToCsv(infos);
		writeToExcel(infos);
	}

	private static void analyze(@NotNull Scanner scanner, List<Info> infos, Date startLog, Date endLog) throws ParseException {
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			if (!StringUtils.endsWith(line, DELIMITER)) {
				continue;
			}
			String guid = StringUtils.substringBetween(line, PATTERN_INFO, StringUtils.SPACE);
			if (!scanner.hasNextLine()) {
				continue;
			}
			Date timeLog = TIME_LOG_FORMAT_05.parse(StringUtils.substringBefore(line, DEBUG));
			if (timeLog.before(startLog)) {
				startLog.setTime(timeLog.getTime());
			}
			if (timeLog.after(endLog)) {
				endLog.setTime(timeLog.getTime());
			}
			// убираем "\ ", далее имя перехода и время перехода
			String docType = StringUtils.substringBetween(line, "документа ", ":");
			String threadName = StringUtils.substringBetween(line, "[", "]");
			line = StringUtils.substring(scanner.nextLine(), 2);
			String transitionName = StringUtils.substringBefore(line, StringUtils.SPACE);
			int time = Integer.parseInt(StringUtils.trim(StringUtils.substringBetween(line, transitionName, MS)));

			infos.add(new Info(guid, threadName, docType, transitionName, time));
		}
	}

	private static String formatTransitionCounts(@NotNull List<Info> infos) {
		return infos.stream()
				.collect(Collectors.groupingBy(
						Info::transitionName,
						Collectors.summingInt(i -> 1)
				))
				.entrySet()
				.stream()
				.map(e -> new TransitionCount(e.getKey(), e.getValue()))
				.sorted() // использует Comparable<TransitionCount>
				.map(TransitionCount::toString)
				.collect(Collectors.joining("\n\t", "Переходы:\n\t", ""));
	}

	/// Альтернативно, чтобы максимально избежать создания объектов TransitionCount, можно описать компаратор вручную:
	private static String formatTransitionCounts1(@NotNull List<Info> infos) {
		return infos.stream()
				.collect(Collectors.groupingBy(
						Info::transitionName,
						Collectors.summingInt(i -> 1)
				))
				.entrySet()
				.stream()
				.sorted(Map.Entry.<String, Integer>comparingByValue(Comparator.reverseOrder())
						.thenComparing(Map.Entry.comparingByKey()))
				.map(e -> e.getKey() + ": " + e.getValue())
				.collect(Collectors.joining("\n\t", "Переходы:\n\t", ""));
	}

	private static void writeToCsv(@NotNull List<Info> infos) {
		logDebug("Запись в файл: " + PATH_TO_LOG_FILE + DETAIL_LOG_CSV);
		try (CSVWriter writer = new CSVWriter(new FileWriter(PATH_TO_LOG_FILE + DETAIL_LOG_CSV))) {
			// Заголовки столбцов
			writer.writeNext(new String[]{
					"Document Type",
					"Transition",
					"Time (ms)",
					"GUID",
					"Thread Name"
			});

			// Данные
			infos.stream().map(info -> new String[]{
					info.docType(),
					info.transitionName(),
					String.valueOf(info.time()),
					info.guid(),
					info.threadName()
			}).forEach(writer::writeNext);
			logDebug("Данные успешно записаны в файл: " + DETAIL_LOG_CSV);
		} catch (IOException e) {
			logError("Ошибка при записи в файл: " + e.getMessage());
		}
	}

	private static void writeToExcel(List<Info> infos) {
		String fileName = PATH_TO_LOG_FILE + DETAIL_LOG_XLSX;
		logDebug("Запись в файл: " + fileName);
		try (Workbook workbook = new XSSFWorkbook()) {
			Sheet sheet = workbook.createSheet("Execution Info");

			// Стиль для заголовков
			CellStyle headerStyle = workbook.createCellStyle();
			Font headerFont = workbook.createFont();
			headerFont.setBold(true);
			headerStyle.setFont(headerFont);

			// Заголовки
			Row headerRow = sheet.createRow(0);
			String[] headers = {"Document Type", "Transition", "Time (ms)", "GUID", "Thread Name"};
			for (int i = 0; i < headers.length; i++) {
				Cell cell = headerRow.createCell(i);
				cell.setCellValue(headers[i]);
				cell.setCellStyle(headerStyle);
			}

			// Данные
			int rowNum = 1;
			for (Info info : infos) {
				Row row = sheet.createRow(rowNum++);
				row.createCell(0).setCellValue(info.docType());
				row.createCell(1).setCellValue(info.transitionName());
				row.createCell(2).setCellValue(info.time());
				row.createCell(3).setCellValue(info.guid());
				row.createCell(4).setCellValue(info.threadName());
			}

			// Авто-размер колонок
			for (int i = 0; i < headers.length; i++) {
				sheet.autoSizeColumn(i);
			}

			// Сохранение
			try (FileOutputStream fos = new FileOutputStream(fileName)) {
				workbook.write(fos);
			}
			logDebug("Данные успешно записаны в файл: " + fileName);
		} catch (IOException e) {
			logError("Ошибка при записи в файл: " + e.getMessage());
		}
	}

	private record Info(String guid, String threadName, String docType, String transitionName,
						int time) implements Comparable<Info> {
		@Override
		public int compareTo(@NotNull Info other) {
			return Integer.compare(other.time, time);
		}

		@Contract(pure = true)
		@Override
		public @NotNull String toString() {
			return String.format("%-25s %-30s %10s %-38s %s",
					docType + ":",
					transitionName,
					time + "ms",
					"[" + guid + "]",
					"[" + threadName + "]"
			);
		}
	}
}
