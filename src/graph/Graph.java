package graph;

import javax.swing.JFrame;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.CategoryDataset;

public class Graph extends JFrame {

	private static final long serialVersionUID = 1L;
	private static String title = "Andamento";
	private String x_axis_label = "Data";
	private String y_axis_label = "";

	public Graph(CategoryDataset dataset) {
		super(title);
		JFreeChart chart = createChart(dataset);
		ChartPanel chartPanel = new ChartPanel(chart);
		chartPanel.setPreferredSize(new java.awt.Dimension(500, 270));
		setContentPane(chartPanel);
	}

	private JFreeChart createChart(CategoryDataset dataset) {
		JFreeChart chart = ChartFactory.createLineChart(title, x_axis_label,
				y_axis_label, dataset, PlotOrientation.VERTICAL, true, true,
				false);
		return chart;
	}

}
