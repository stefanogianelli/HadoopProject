package graph;

import javax.swing.JFrame;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.CategoryLabelPositions;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.CategoryDataset;
import org.jfree.ui.RefineryUtilities;

public class BarChart extends JFrame implements Runnable {

	private static final long serialVersionUID = 1L;
	
	private static final int width = 1500;
	private static final int height = 800;
	
	private static String title = "Domini";
	private String x_axis_label = "Date";
	private String y_axis_label = "Valori";
	private CategoryDataset dataset;

	public BarChart (CategoryDataset dataset) {
		super(title);
		this.dataset = dataset;
	}

	private JFreeChart createChart(CategoryDataset dataset) {
		JFreeChart chart = ChartFactory.createBarChart(title, x_axis_label,
				y_axis_label, dataset, PlotOrientation.VERTICAL, true, true, false);
		chart.removeLegend();
		CategoryPlot plot = chart.getCategoryPlot();
		CategoryAxis domainAxis = plot.getDomainAxis();
        domainAxis.setCategoryLabelPositions(
            CategoryLabelPositions.createUpRotationLabelPositions(Math.PI / 6.0)
        );
		return chart;
	}

	@Override
	public void run() {
		JFreeChart chart = createChart(dataset);
		ChartPanel chartPanel = new ChartPanel(chart);
		chartPanel.setFillZoomRectangle(true);
		chartPanel.setMouseWheelEnabled(true);
		chartPanel.setPreferredSize(new java.awt.Dimension(width, height));
		setContentPane(chartPanel);
		this.pack();
		RefineryUtilities.centerFrameOnScreen(this);		
		this.setVisible(true);
	}

}
