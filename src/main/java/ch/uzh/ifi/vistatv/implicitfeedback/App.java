package ch.uzh.ifi.vistatv.implicitfeedback;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class App {
	public static void main(String[] args) {
		try {
			BBCRatingGenerator rg = new BBCRatingGenerator(
					new FileReader(
							"/Users/faber/Documents/Job-ifi/dataset/bbc/epg_data_with_tabs.csv"));

			rg.generate(
					new FileReader(
							"/Users/faber/Documents/Job-ifi/dataset/bbc/all_fractions_with_tabs.csv"),
					null,
					new FileWriter(
							"/Users/faber/Documents/Job-ifi/dataset/bbc/all_fractions_with_implicit_ratings.csv"));

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
