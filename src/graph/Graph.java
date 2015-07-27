package graph;

import javax.swing.JFrame;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.data.xy.XYDataset;

public class Graph extends JFrame {

	private static final long serialVersionUID = 1L;
	private static final int width = 1500;
	private static final int height = 800;
	
	private static String title = "Andamento";
	private String x_axis_label = "Date";
	private String y_axis_label = "Valori";

	public Graph(XYDataset dataset) {
		super(title);
		JFreeChart chart = createChart(dataset);
		ChartPanel chartPanel = new ChartPanel(chart);
		chartPanel.setFillZoomRectangle(true);
		chartPanel.setMouseWheelEnabled(true);
		chartPanel.setPreferredSize(new java.awt.Dimension(width, height));
		setContentPane(chartPanel);
	}

	private JFreeChart createChart(XYDataset dataset) {
		JFreeChart chart = ChartFactory.createTimeSeriesChart(title, x_axis_label,
				y_axis_label, dataset, true, true, false);
		return chart;
	}

}
