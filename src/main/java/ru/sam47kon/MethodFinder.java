package ru.sam47kon;

import lombok.Value;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class MethodFinder {
	private static final String XML_PATH = "D:\\doc\\projects\\gitlab\\05\\ufos-func-05\\Func\\config\\lifecycles";

	public static void main(String[] args) throws Exception {
		List<Result> results = searchXmlFiles();
		results.forEach(r -> System.out.println(r.file + ":" + r.lineNumber));
	}

	static @NotNull List<Result> searchXmlFiles() throws Exception {
		List<Result> results = new ArrayList<>();
		Files.walk(Paths.get(XML_PATH))
				.filter(p -> p.toString().endsWith(".lc"))
				.forEach(p -> processFile(p.toFile(), results));
		return results;
	}

	static void processFile(File file, List<Result> results) {
		try (FileReader reader = new FileReader(file)) {
			XMLStreamReader xmlReader = XMLInputFactory.newInstance()
					.createXMLStreamReader(reader);

			boolean inTargetCall = false;
			boolean hasCurrentGuid = false;
			boolean hasInvalidSourceGuid = false;
			int callStartLine = -1;

			while (xmlReader.hasNext()) {
				int event = xmlReader.next();
				if (event == XMLStreamConstants.START_ELEMENT) {
					if ("call-ref".equals(xmlReader.getLocalName())) {
						String id = getAttributeValue(xmlReader, "id");
						// updateFundsInFundsJournalLCAllnew findFundsInFundsJournalCall
						if ("updateFundsInFundsJournalLCAllnew".equals(id)) {
							inTargetCall = true;
							callStartLine = xmlReader.getLocation().getLineNumber();
						}
					}

					if (inTargetCall && "parameter".equals(xmlReader.getLocalName())) {
						String name = getAttributeValue(xmlReader, "name");
						String value = getAttributeValue(xmlReader, "value");

						if ("findByCurrentDocGuid".equals(name) && "true".equals(value)) {
							hasCurrentGuid = true;
						} else if ("findBySourceDocGuid".equals(name) && "true".equals(value)) {
							hasInvalidSourceGuid = true;
						}
					}
				}

				if (event == XMLStreamConstants.END_ELEMENT
						&& "call-ref".equals(xmlReader.getLocalName())
						&& inTargetCall) {
					if (hasCurrentGuid && !hasInvalidSourceGuid) {
						results.add(new Result(file.getAbsolutePath(), callStartLine));
					}
					inTargetCall = false;
					hasCurrentGuid = false;
					hasInvalidSourceGuid = false;
				}
			}
		} catch (Exception e) {
			System.err.println("Ошибка обработки файла " + file.getAbsolutePath() + ": " + e.getMessage());
		}
	}

	private static @Nullable String getAttributeValue(@NotNull XMLStreamReader reader, String attrName) {
		for (int i = 0; i < reader.getAttributeCount(); i++) {
			if (attrName.equals(reader.getAttributeLocalName(i))) {
				return reader.getAttributeValue(i);
			}
		}
		return null;
	}

	@Value
	static class Result {
		String file;
		int lineNumber;
	}
}
