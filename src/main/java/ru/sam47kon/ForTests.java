package ru.sam47kon;

import org.jetbrains.annotations.NotNull;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

public class ForTests {
	public static void main(String[] args) {
		ForTests forTests = new ForTests();

		int concurrentCount = 20;
		int groupsSize = 50;
		double ceil = Math.ceil((1.0d * groupsSize) / concurrentCount);
		int maxPoolSize = (int) (ceil > 1.0d ? concurrentCount * ceil : concurrentCount + ceil);
		System.out.println(maxPoolSize);

		//System.out.println(forTests.generateReport("2023-01-01:product1:10;2023-01-01:product2:20;2023-01-02:product1:30;2023-01-02:product2:40"));
	}

	public String generateReport(String salesData) {
		if (salesData == null || salesData.isEmpty()) {
			return "";
		}

		StringTokenizer tokenizer = new StringTokenizer(salesData, ";");
		SalesData data = new SalesData();
		while (tokenizer.hasMoreTokens()) {
			String salesByDay = tokenizer.nextToken();
			String[] dataSplit = salesByDay.split(":");
			if (dataSplit.length != 3) {
				throw new IllegalArgumentException("Invalid data format: " + salesByDay);
			}
			try {
				int month = LocalDate.parse(dataSplit[0]).getMonthValue();
				int count = Integer.parseInt(dataSplit[2]);
				data.put(month, dataSplit[1], count);
			} catch (Exception e) {
				throw new IllegalArgumentException("Error processing data: " + salesByDay, e);
			}
		}
		return data.toString();
	}

	private static class SalesData {
		private static final int Q1_END = 3;
		private static final int Q2_END = 6;
		private static final int Q3_END = 9;
		private static final String[] QUARTERS = {"Q1", "Q2", "Q3", "Q4"};

		Map<String, Integer> Q1 = new HashMap<>();
		Map<String, Integer> Q2 = new HashMap<>();
		Map<String, Integer> Q3 = new HashMap<>();
		Map<String, Integer> Q4 = new HashMap<>();

		void put(int month, String productName, int count) {
			if (month <= Q1_END) {
				Q1.merge(productName, count, Integer::sum);
			} else if (month <= Q2_END) {
				Q2.merge(productName, count, Integer::sum);
			} else if (month <= Q3_END) {
				Q3.merge(productName, count, Integer::sum);
			} else {
				Q4.merge(productName, count, Integer::sum);
			}
		}

		@Override
		public String toString() {
			StringBuilder result = new StringBuilder();
			for (int i = 0; i < QUARTERS.length; i++) {
				Map<String, Integer> quarterMap = getQuarterMap(i);
				if (!quarterMap.isEmpty()) {
					result.append(QUARTERS[i]).append(":").append(toString(quarterMap)).append("\n");
				}
			}
			return result.toString();
		}

		private Map<String, Integer> getQuarterMap(int index) {
			return switch (index) {
				case 0 -> Q1;
				case 1 -> Q2;
				case 2 -> Q3;
				case 3 -> Q4;
				default -> throw new IllegalArgumentException("Invalid quarter index: " + index);
			};
		}

		private @NotNull String toString(@NotNull Map<String, Integer> map) {
			return map.entrySet().stream()
					.map(e -> "- " + e.getKey() + ": " + e.getValue())
					.collect(Collectors.joining("\n", "\n", ""));
		}
	}
}