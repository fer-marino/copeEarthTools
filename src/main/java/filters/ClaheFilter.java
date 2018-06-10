package filters;

// Catalano Imaging Library
// The Catalano Framework
//
// Copyright © Diego Catalano, 2012-2016
// diego.catalano at live.com
//
// Copyright © Stephan Saafeld, 2013
// saalfeld at mpi-cbg.de
//
//    This library is free software; you can redistribute it and/or
//    modify it under the terms of the GNU Lesser General Public
//    License as published by the Free Software Foundation; either
//    version 2.1 of the License, or (at your option) any later version.
//
//    This library is distributed in the hope that it will be useful,
//    but WITHOUT ANY WARRANTY; without even the implied warranty of
//    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
//    Lesser General Public License for more details.
//
//    You should have received a copy of the GNU Lesser General Public
//    License along with this library; if not, write to the Free Software
//    Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
//


import org.gdal.gdal.Band;
import org.gdal.gdal.Dataset;
import org.gdal.gdalconst.gdalconst;

/**
 * Contrast Limited Adaptive Histogram Equalization.
 *
 * <p>This feature can also be applied to global histogram equalization, giving rise to
 * contrast limited histogram equalization (CLHE) which is rarely used in practice.</p>
 *
 * <br>References: http://en.wikipedia.org/wiki/Adaptive_histogram_equalization#Contrast_Limited_AHE
 *
 * <p><li>Supported types: Grayscale, RGB.
 * <br><li>Coordinate System: Matrix.
 *
 * @author Diego Catalano
 */
public class ClaheFilter {

    private int blockRadius = 63;
    private int bins = 255;
    private float slope = 3f;

    /**
     * Get Block radius.
     * The size of the local region around a pixel for which the histogram is equalized.
     * This size should be larger than the size of features to be preserved.
     *
     * @return Size of block radius.
     */
    public int getBlockRadius() {
        return blockRadius;
    }

    /**
     * Set Block radius.
     * The size of the local region around a pixel for which the histogram is equalized.
     * This size should be larger than the size of features to be preserved.
     *
     * @param blockRadius Size of block radius.
     */
    public void setBlockRadius(int blockRadius) {
        this.blockRadius = blockRadius;
    }

    /**
     * Get the number of histogram bins used for histogram equalization.
     *
     * @return Number of histogram bins.
     */
    public int getBins() {
        return bins;
    }

    /**
     * Set the number of histogram bins used for histogram equalization.
     *
     * @param bins Number of histogram bins.
     */
    public void setBins(int bins) {
        this.bins = bins;
    }

    /**
     * Get Slope.
     * Limits the contrast stretch in the intensity transfer function. Very large values will let the
     * histogram equalization do whatever it wants to do, that is result in maximal local contrast.
     * The value 1 will result in the original image.
     *
     * @return Slope.
     */
    public float getSlope() {
        return slope;
    }

    /**
     * Set Slope.
     * Limits the contrast stretch in the intensity transfer function. Very large values will let the
     * histogram equalization do whatever it wants to do, that is result in maximal local contrast.
     * The value 1 will result in the original image.
     *
     * @param slope Slope.
     */
    public void setSlope(float slope) {
        this.slope = slope;
    }


    /**
     * Initialize a new instance of the CLAHE class.
     * <br><br>Block radius: 63.
     * <br>Bins: 255.
     * <br>Slope: 3.
     */
    public Clahe() {
    }

    /**
     * Initialize a new instance of the CLAHE class.
     *
     * @param blockRadius Block radius.
     * @param bins        Bins.
     */
    public Clahe(int blockRadius, int bins) {
        this.blockRadius = blockRadius;
        this.bins = bins;
    }

    /**
     * Initialize a new instance of the CLAHE class.
     *
     * @param blockRadius Block radius.
     * @param bins        Bins.
     * @param slope       Slope.
     */
    public Clahe(int blockRadius, int bins, float slope) {
        this.blockRadius = blockRadius;
        this.bins = bins;
        this.slope = slope;
    }


    public void apply(Dataset input) {
        int width = input.getRasterXSize();
        int height = input.getRasterYSize();

        for (int b = 0; b < input.getRasterCount(); b++) {
            byte[] rasterData = new byte[width * height];
            Band band = null;

            if (input.getRasterCount() == 1) {
                band = input.GetRasterBand(0);

            } else {
                // TODO convert to gray scale and apply the algorithm

            }

            double[] stats = new double[5];
            band.ComputeBandStats(stats);

            int dataType = band.getDataType();
            if(dataType == gdalconst.GDT_Byte) {
                int res = band.ReadRaster(0, 0, width, height, rasterData);
            } else if(dataType == gdalconst.GDT_CInt16) {
                short[] tmpBuffer = new short[width*height];
                int res = band.ReadRaster(0, 0, width, height, tmpBuffer);
                for (int i = 0; i < tmpBuffer.length; i++) {
                    rasterData[i] = Short.valueOf(tmpBuffer[i]).byteValue(); // TODO rescale
                }
            } else
                throw new IllegalArgumentException("Unsupported data type");


            apply(width, height, rasterData);

            band.WriteRaster(0, 0, width, height, rasterData);
        }
    }

    private void apply(int width, int height, byte[] byteBuffer) {

        for (int i = 0; i < height; i++) {

            int[][] result = new int[height][width];

            int iMin = Math.max(0, i - blockRadius);
            int iMax = Math.min(height, i + blockRadius + 1);
            int h = iMax - iMin;

            int jMin = Math.max(0, -blockRadius);
            int jMax = Math.min(width - 1, blockRadius);

            int[] hist = new int[bins + 1];
            int[] clippedHist = new int[bins + 1];

            for (int k = iMin; k < iMax; k++)
                for (int l = jMin; l < jMax; l++)
                    ++hist[(int) Math.ceil(byteBuffer[k + 1] / 255.0f * bins)];
//                    ++hist[Math.ceil(input.getGray(k, l) / 255.0f * bins)];

            for (int j = 0; j < width; j++) {
                int v = (int) (Math.ceil(byteBuffer[i * j + j]) / 255.0f * bins);
//                int v = Math.ceil(input.getGray(i, j) / 255.0f * bins);

                int xMin = Math.max(0, j - blockRadius);
                int xMax = j + blockRadius + 1;
                int w = Math.min(width, xMax) - xMin;
                int n = h * w;

                int limit = (int) (slope * n / bins + 0.5f);

                /* remove left behind values from histogram */
                if (xMin > 0) {
                    int xMin1 = xMin - 1;
                    for (int yi = iMin; yi < iMax; ++yi)
                        --hist[(int) Math.ceil(byteBuffer[yi * xMin1 + xMin1] / 255.0f * bins)];
//                        --hist[Math.ceil(input.getGray(yi, xMin1) / 255.0f * bins)];
                }

                /* add newly included values to histogram */
                if (xMax <= width) {
                    int xMax1 = xMax - 1;
                    for (int yi = iMin; yi < iMax; ++yi)
                        ++hist[(int) Math.ceil(byteBuffer[yi + xMax1 + xMax1] / 255.0f * bins)];
//                        ++hist[Math.ceil(input.getGray(yi, xMax1) / 255.0f * bins)];
                }

                System.arraycopy(hist, 0, clippedHist, 0, hist.length);
                int clippedEntries = 0, clippedEntriesBefore;

                do {
                    clippedEntriesBefore = clippedEntries;
                    clippedEntries = 0;
                    for (int z = 0; z <= bins; ++z) {
                        int d = clippedHist[z] - limit;
                        if (d > 0) {
                            clippedEntries += d;
                            clippedHist[z] = limit;
                        }
                    }

                    int d = clippedEntries / (bins + 1);
                    int m = clippedEntries % (bins + 1);
                    for (int z = 0; z <= bins; ++z)
                        clippedHist[z] += d;

                    if (m != 0) {
                        int s = bins / m;
                        for (int z = 0; z <= bins; z += s)
                            ++clippedHist[z];
                    }
                }
                while (clippedEntries != clippedEntriesBefore);

                /* build cdf of clipped histogram */
                int hMin = bins;
                for (int z = 0; z < hMin; ++z)
                    if (clippedHist[z] != 0) hMin = z;

                int cdf = 0;
                for (int z = hMin; z <= v; ++z)
                    cdf += clippedHist[z];

                int cdfMax = cdf;
                for (int z = v + 1; z <= bins; ++z)
                    cdfMax += clippedHist[z];

                int cdfMin = clippedHist[hMin];

                result[i][j] = (int) Math.ceil((cdf - cdfMin) / (float) (cdfMax - cdfMin) * 255.0f);
            }

            for (int a = 0; a < width; a++)
                byteBuffer[i * a + a] = (byte) result[i][a];

        }

    }


}