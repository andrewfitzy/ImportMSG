package com.about80minutes.palantir.cedt.msg;

import java.net.URI;

import com.palantir.api.dataintegration.detect.AbstractDetector;
import com.palantir.api.dataintegration.detect.DetectionResult;
import com.palantir.api.dataintegration.detect.DetectorContext;
import com.palantir.api.dataintegration.detect.StopDetectionException;
import com.palantir.api.dataintegration.util.ContentProvider;

/**
 * Class that implements the detector functionality for the CEDT framework, this
 * returns a positive result for files who's names end .msg, if one is found
 * then MSGTransformer.NAME is returned as the detection result, otherwise
 * evaluation will pass to the next detector.
 */
public class MSGDetector extends AbstractDetector<ContentProvider> {
	/**
	 * Constant used to identify the name of the detector registry which this detector belongs to.
	 */
	public static final String REGISTRY_NAME = "MSGDetectorRegistry";

	/**
	 * Method to perform detection for a given set of circumstances. This method
	 * evaluates the parameters to decide what the detection result will be, if
	 * any of the criteria are satisfied an instance of DetectionResult will
	 * be returned which will contain the name of the transformer to use.
	 *
	 * @param uri a {@link java.net.URI} which describes the resource being imported
	 * @param context a {@link com.palantir.api.dataintegration.detect.DetectorContext}
	 * which provides the context for this import
	 * @param value a {com.palantir.api.dataintegration.util.ContentProvider}
	 * which describes the object providing content to this detection
	 *
	 * @return a {@link com.palantir.api.dataintegration.detect.DetectionResult} which contains the name of the transformer to use.
	 *
	 * @throws StopDetectionException if there is an error during detection
	 */
	public DetectionResult detect(URI uri, DetectorContext context, ContentProvider value) throws StopDetectionException {
		DetectionResult result = null;

		// Check for the file name extension ".msg"
		if(value.getFullName().toUpperCase().endsWith(".MSG")) {
			result = new DetectionResult(MSGTransformer.NAME, "com.palantir.datasource.fileemail");
		}
		// If the file name extension isn’t ".msg", return null and the next detector can try to figure it out.
		return result;
	}
}
