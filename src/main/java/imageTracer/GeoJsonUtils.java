package imageTracer;

import org.apache.commons.math3.linear.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

public class GeoJsonUtils {

    private static float round(double val, float places){
        return (float)(Math.round(val*Math.pow(10,places))/Math.pow(10,places));
    }

    private static void geoJsonPolygon(StringBuilder sb, ArrayList<Double[]> segments, String colorstr, HashMap<String,Float> options) {
        float roundCoords = (float) Math.floor(options.get("roundcoords"));
        // Path
        sb.append("<path ").append(colorstr).append("d=\"" ).append("M ").append(segments.get(0)[1]).append(" ").append(segments.get(0)[2]).append(" ");

        if( roundCoords == -1 ){
            for (Double[] segment : segments) {
                if (segment[0] == 1.0) { // line to
                    sb.append("L ").append(segment[3] ).append(" ").append(segment[4] ).append(" ");
                } else { // bezier curve to
                    sb.append("Q ").append(segment[3] ).append(" ").append(segment[4] ).append(" ").append(segment[5] ).append(" ").append(segment[6] ).append(" ");
                }
            }
        }else{
            for (Double[] segment : segments) {
                if (segment[0] == 1.0) {
                    sb.append("L ").append(round(segment[3], roundCoords)).append(" ")
                            .append(round(segment[4], roundCoords)).append(" ");
                } else {
                    sb.append("Q ").append(round(segment[3], roundCoords)).append(" ")
                            .append(round(segment[4], roundCoords)).append(" ")
                            .append(round(segment[5], roundCoords)).append(" ")
                            .append(round(segment[6], roundCoords)).append(" ");
                }
            }
        }// End of roundcoords check

        sb.append("Z\" />");

    }

    // Converting tracedata to an geojson string, paths are drawn according to a Z-index
    public static String getGeojson (ImageTracer.IndexedImage ii, HashMap<String,Float> options){
        // SVG start
        int w = ii.width, h = ii.height;
        StringBuilder svgstr = new StringBuilder("{ \"type\": \"FeatureCollection\",\n \tfeatures\": [");

        // creating Z-index
        TreeMap<Double,Integer[]> zindex = new TreeMap<>();
        double label;
        // Layer loop
        for(int k=0; k<ii.layers.size(); k++) {

            // Path loop
            for(int pcnt=0; pcnt<ii.layers.get(k).size(); pcnt++){

                // Label (Z-index key) is the startpoint of the path, linearized
                label = (ii.layers.get(k).get(pcnt).get(0)[2] * w) + ii.layers.get(k).get(pcnt).get(0)[1];
                // Creating new list if required
                if(!zindex.containsKey(label)){ zindex.put(label,new Integer[2]); }
                // Adding layer and path number to list
                zindex.get(label)[0] = k;
                zindex.get(label)[1] = pcnt;
            }// End of path loop

        }// End of layer loop

        // Sorting Z-index is not required, TreeMap is sorted automatically

        // Drawing
        // Z-index loop
        for(Map.Entry<Double, Integer[]> entry : zindex.entrySet()) {
            geoJsonPolygon(svgstr,
                    ii.layers.get(entry.getValue()[0]).get(entry.getValue()[1]),
                    geoJsonColor(ii.palette[entry.getValue()[0]]),
                    options);
        }

        // SVG End
        svgstr.append("\t]\n}");

        return svgstr.toString();

    }// End of getsvgstring()

    private static String geoJsonColor(byte[] c){
        return "\"properties\": { fill: \"rgb("+(c[0]+128)+","+(c[1]+128)+","+(c[2]+128)+")\" stroke: \"rgb("+(c[0]+128)+","+(c[1]+128)+","+(c[2]+128)+")\" stroke-width: \"1\" fill-opacity: \""+((c[3]+128)/255.0)+"\" }";
    }

    /**
     * tells path rotation, clockwise or counter-clockwise
     * @param path the path to check
     * @return -1 for clockwise, 1 for counterclockwise, 0 for mixed
     */
    private int checkRotation(ArrayList<Double[]> path) {
        RealMatrix pathM = new Array2DRowRealMatrix(path.size(), 3);
        final AtomicInteger row = new AtomicInteger();
        path.forEach(e -> pathM.setRow(row.getAndIncrement(), new double[]{e[3], e[4], 0}));

        // outer for probably not needed
        RealMatrix m1 = pathM.getSubMatrix(0, pathM.getRowDimension()-2, 0 ,2);
        RealMatrix m2 = pathM.getSubMatrix(1, pathM.getRowDimension()-1, 0 ,2);

        RealMatrix ris = m1.multiply(m2);
        double[] signs = new double[ris.getRowDimension()];
        for(int r = 0; r < ris.getRowDimension(); r++)
            signs[r] = Math.signum(ris.getEntry(r, 2));

        double first = signs[0];
        boolean mixed = false;
        for (int i = 1; i < signs.length; i++)
            if(signs[i] != first) {
                mixed = true;
                break;
            }

        if(!mixed)
            return (int) first;
        else
            return 0;
    }
}
