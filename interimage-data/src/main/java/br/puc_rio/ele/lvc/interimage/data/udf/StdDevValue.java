package br.puc_rio.ele.lvc.interimage.data.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import br.puc_rio.ele.lvc.interimage.data.DataParser;
import net.imglib2.img.Img;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.ops.operation.iterable.unary.StdDeviation;;

/**
 * A UDF that returns the standard deviation of a raster layer.
 * Example:
 * 		A = load 'mydata' as (raster);
 * 		B = foreach A generate StdDevValue(raster#'0');
 * @author Patrick Happ
 * @author Rodrigo Ferreira
 *
 */
public class StdDevValue extends EvalFunc<Double> {
	
	private final DataParser imageParser = new DataParser();
	
	/**
     * Method invoked on every tuple during foreach evaluation
     * @param input tuple; first column is assumed to have the raster layer
     * @exception java.io.IOException
     * @return standard deviation of the layer, or null in case of processing error
     */
	@Override
	public Double exec(Tuple input) throws IOException {		
		if (input == null || input.size() == 0)
            return null;
        
		try {			
			Object objImage = input.get(0);
			//TODO this should be generic and not only for DoubleType
			Img <DoubleType> ip = imageParser.parseData(objImage);
			DoubleType value =  new DoubleType();;
			
			StdDeviation< DoubleType, DoubleType >  res = new StdDeviation< DoubleType, DoubleType >();
			return res.compute(ip.iterator(), value).get();

		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}
	
	@Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.DOUBLE));
    }
	
}