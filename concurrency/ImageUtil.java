/**
 * Copyright (c) 2013 Amazon.com, Inc.  All rights reserved.
 *
 * Owner: search-experience@
 */
package concurrency;

import amazon.media.result.ProductResult;
import amazon.media.result.ResultBatch;
import amazon.media.util.MediaIOException;
import amazon.media.util.Rendering;
import amazon.media.util.StickerLocation;
import com.amazon.searchapp.render.adp.IPropertyAccessor;
import com.amazon.searchapp.render.pagelet.json.MSAUtils;
import com.amazon.searchapp.render.properties.ISearchAliasProperties;
import com.amazon.searchapp.render.properties.types.ImageScaling;
import com.amazon.searchapp.render.properties.types.ImageScalingStrategy;
import com.amazon.searchapp.render.properties.types.ImageSize;
import com.amazon.searchapp.render.service.media.IMediaHelper;
import com.amazon.searchapp.render.service.product.IProduct;
import com.amazon.searchapp.render.service.sirius.SeriesSearchUtils;
import com.amazon.searchapp.render.types.*;
import com.amazon.searchapp.render.util.beans.SearchWeblabClient;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


/**
 * Image utilities
 */
public final class ImageUtil
{
    private static final Logger LOGGER = Logger.getLogger(ImageUtil.class);

    private static final ConcurrentHashMap<Integer, ConcurrentHashMap<ImageSize, Rendering>> SITB_STICKERING_RENDERING_MAP =
        new ConcurrentHashMap<Integer, ConcurrentHashMap<ImageSize, Rendering>>(10);

    private static final ConcurrentHashMap<Integer, Rendering> SQUARE_RENDERING_MAP =
        new ConcurrentHashMap<Integer, Rendering>(10);

    private static final ConcurrentHashMap<Integer, Rendering> RECTANGULAR_SCALING_RENDERING_MAP_BY_WIDTH =
        new ConcurrentHashMap<Integer, Rendering>(10);

    private static final ConcurrentHashMap<Integer, Rendering> RECTANGULAR_SCALING_RENDERING_MAP_BY_HEIGHT =
        new ConcurrentHashMap<Integer, Rendering>(10);

    private static final ConcurrentHashMap<Integer, Rendering> RECTANGULAR_SCALING_RENDERING_MAP_FOR_SQUARE =
        new ConcurrentHashMap<Integer, Rendering>(10);

    // see convertToIntegerKey method for how key is constructed
    private static final ConcurrentHashMap<Integer, Rendering> SCALE_TO_WIDTH_CROP_MAP =
        new ConcurrentHashMap<Integer, Rendering>(10);

    // see convertToIntegerKey method for how key is constructed
    private static final ConcurrentHashMap<Integer, Rendering> SCALE_TO_HEIGHT_CROP_MAP =
        new ConcurrentHashMap<Integer, Rendering>(10);

    private static final ConcurrentHashMap<Integer, Rendering> SCALE_TO_LONGEST_SIDE_MAP =
        new ConcurrentHashMap<Integer, Rendering>(10);

    private static final ConcurrentHashMap<Integer, Rendering> AUTOCROP_RESIZE_MAP =
        new ConcurrentHashMap<Integer, Rendering>(10);

    private static final ConcurrentHashMap<Integer, Rendering> AUTOCROP_UPSCALE_RESIZE_MAP =
        new ConcurrentHashMap<Integer, Rendering>(10);

    private static final int SCALE_SIZE = 350;
    private static final String RENDERING_AUTO_CROP_TAG = "AC";
    private static final String RENDERING_SPRITE_TAG = "SP";
    private static final String RENDERING_VALUES_DELIMITER = ",";
    public static final String PRIME_SASH = "_PJPrime-Sash-Extra-Large-2017";

    private static final Pattern STYLE_CODES_EXTRACTOR = Pattern.compile(".*\\._(.*)_\\.(jpg|png|gif)");
    private static final String STYLE_CODES_DELIMITER = "_";

    
    /**
     * weblabs for various (platform, image format) pairs
     *        PC/Tablet     Mobile        SRDS
     *  ----------------------------------------------
     *  webp  SEARCH_50614  SEARCH_50616  SEARCH_50618    
     *  -------------------------------------
     *  jpeg  SEARCH_50613  SEARCH_50615  SEARCH_50617    
     */

    private static final int HIGH_DENSITY_IMAGE_COMPRESSION_RATIO = 65;
    
    /**
     * Takes a base image that is hopefully larger than 160px
     * and applies the SITB sticker.
     * Sticker media tags stolen from the RPA here (P4DB):
     * http://tiny/wpw/p4dbamazsourp4dbbrazfile
     *
     * If image url is empty, then returns image without sticker.
     *
     * @param image     the source rendering image
     * @param orgUnit   the orgUnit we are inside
     * @param mediaHelper   media helper, used to rewrite url with style codes
     */
    public static void applySitbSticker(RenderImage image, int orgUnit, IMediaHelper mediaHelper, ImageSize imageSize)
    {
        Validate.notNull(image, "image parameter required");
        Validate.notNull(mediaHelper, "mediaClient parameter required");

        if (StringUtils.isEmpty(image.getUrl()))
        {
            return;
        }
        Rendering rendering = getSitbStickeringRendering(orgUnit, imageSize);
        image.setUrl(mediaHelper.appendRendering(image.getUrl(), rendering));
    }


    /**
     * Resize the image and apply additional media styles on a per-request basis.
     * Calls {@link #resizeImage(RenderImage, ImageSize, boolean, IMediaHelper)} to scale the image.
     *
     * @param image
     * @param imageSize
     * @param scaleExact
     * @param mediaHelper
     * @param product
     * @param requestContext
     * @param saDisplayProps
     * @param propertyAccessor
     */
    public static void renderProductImage(RenderImage image, ImageSize imageSize, boolean scaleExact, IMediaHelper mediaHelper,
                                          IProduct product, IRequestContext requestContext, ISearchAliasProperties saDisplayProps,
                                          IPropertyAccessor propertyAccessor)
    {
        renderProductImage(image, imageSize, scaleExact, mediaHelper, product, requestContext, saDisplayProps, propertyAccessor, false);
    }

    /**
     * Resize the image and apply additional media styles on a per-request basis.
     * Calls {@link #resizeImage(RenderImage, ImageSize, boolean, IMediaHelper)} to scale the image.
     *
     * @param image
     * @param imageSize
     * @param scaleExact
     * @param mediaHelper
     * @param product
     * @param requestContext
     * @param saDisplayProps
     * @param propertyAccessor
     * @param includeHiResImage
     */
    public static void renderProductImage(RenderImage image, ImageSize imageSize, boolean scaleExact, IMediaHelper mediaHelper,
        IProduct product, IRequestContext requestContext, ISearchAliasProperties saDisplayProps,
        IPropertyAccessor propertyAccessor, boolean includeHiResImage)
    {
        renderProductImage(image, imageSize.getImageScaling(), scaleExact, mediaHelper, product,
            requestContext, saDisplayProps, propertyAccessor, includeHiResImage);
    }

    /**
     * Resize and style the image. Define high res image variants.
     */
    public static void renderProductImage(RenderImage image, ImageScaling imageScaling, boolean scaleExact,
        IMediaHelper mediaHelper, IProduct product, IRequestContext requestContext,
        ISearchAliasProperties saDisplayProps, IPropertyAccessor propertyAccessor, boolean includeHiResImage)
    {
        RenderImage sourceImage = new RenderImage(image);

        renderBaseProductImage(image, imageScaling, scaleExact, mediaHelper, product, requestContext,
            saDisplayProps, propertyAccessor, includeHiResImage);

        buildScreenImages(requestContext, saDisplayProps, propertyAccessor, mediaHelper, product, sourceImage,
            image, imageScaling, scaleExact, includeHiResImage);
    }

    /**
     * Resize and style the image
     */
    private static void renderBaseProductImage(RenderImage image, ImageScaling imageScaling, boolean scaleExact,
        IMediaHelper mediaHelper, IProduct product, IRequestContext requestContext,
        ISearchAliasProperties saDisplayProps, IPropertyAccessor propertyAccessor, boolean includeHiResImage)
    {
        if (product != null && !image.isPlaceholder() 
                && (propertyAccessor.getBoolean("autocropAndUpscale") || propertyAccessor.getBoolean("autocropAndResize")))
        {
            boolean upscaleImage = propertyAccessor.getBoolean("autocropAndUpscale");

            //Do not upscale image for series re-rank products. Their image size is 160*218.
            //We only want scaleToRectangle(_SR160,218) but not upscaleToLongest && scaleToRectangle(_UL218_SR160,218) to have higher resolution.
            if (SeriesSearchUtils.hasSeriesVolumeIndex(product.getPageItem()))
            {
                upscaleImage = false;
            }
            autocropAndResizeImage(image, imageScaling.getWidth(), imageScaling.getHeight(), mediaHelper, upscaleImage);
        }
        else
        {
            resizeImage(image, imageScaling, imageScaling.getWidth(), imageScaling.getHeight(), scaleExact,
                    mediaHelper, includeHiResImage);

            if (product != null && propertyAccessor.getBoolean(product, "autocropImage"))
            {
                autocropImage(image, mediaHelper, includeHiResImage);
            }
        }
        finalizeProductImage(image, product, requestContext, propertyAccessor, includeHiResImage);
    }

    /**
     * This method autocrops the image and then resize to square or rectangle using
     * the tags AA or SR<width>,<height> respectively.  SR works better than SL and SX
     * because it keeps the right proportion without distorting the image.
     * UpscaleImage uses the U tags which increase the quality of the image but they
     * may have a latency impact.
     * 
     * @param image
     * @param width
     * @param height
     * @param mediaHelper
     * @param upscaleImage
     */
    public static void autocropAndResizeImage(RenderImage image, int width, int height, IMediaHelper mediaHelper, boolean upscaleImage)
    {
        Validate.notNull(image, "image parameter required");
        Validate.notNull(mediaHelper, "mediaHelper parameter required");

        Rendering rendering = null;
        if (StringUtils.isEmpty(image.getUrl()))
        {
            return;
        }

        Integer key = convertToIntegerKey(width, height);
        rendering = upscaleImage ? AUTOCROP_UPSCALE_RESIZE_MAP.get(key) : AUTOCROP_RESIZE_MAP.get(key);
        
        if (rendering == null)
        {
            rendering = new Rendering().custom(RENDERING_AUTO_CROP_TAG, "");
            if (upscaleImage)
            {
                if (width == height)
                {
                    rendering = rendering.upscaleToSquare(width);
                }
                else
                {
                    rendering = rendering.upscaleToLongest(height).scaleToRectangle(width, height);
                }
                AUTOCROP_UPSCALE_RESIZE_MAP.putIfAbsent(key, rendering);
            }
            else
            {
                if (width == height)
                {
                    rendering = rendering.scaleToSquare(width);
                }
                else
                {
                    rendering = rendering.scaleToRectangle(width, height);
                }
                AUTOCROP_RESIZE_MAP.putIfAbsent(key, rendering);
            }
        }
        image.setHeight(height);
        image.setWidth(width);
        image.setUrl(mediaHelper.appendRendering(image.getUrl(), rendering));
    }

    /**
     * Proxy method for other resize methods in this class. Scales the rendering image based on the supplied parameter.
     * If the target image size specifications are equal in width and height, the image will be no larger than that
     * value in any direction (height or width).  If scaleExact is true and image height or width not exactly
     * same as maxSize, then height and width will be adjusted to maxSize.
     *
     * If the target image size specifications are rectangular, the image will be scaled down and cropped to those
     * exact dimensions
     *
     * If any scaling occurs, then
     * URL is altered to include the relevant style codes.
     *
     * If image URL is empty, then returns image without resizing.     *
     */
    public static void resizeImage(RenderImage image, ImageSize imageSize, boolean scaleExact, IMediaHelper mediaHelper)
    {
        ImageScaling imageScaling = imageSize.getImageScaling();
        resizeImage(image, imageScaling, imageScaling.getWidth(), imageScaling.getHeight(), scaleExact, mediaHelper, false);
    }

    public static void resizeImage(RenderImage image, ImageSize imageSize, int targetWidth, int targetHeight,
        boolean scaleExact, IMediaHelper mediaHelper)
    {
        resizeImage(image, imageSize.getImageScaling(), targetWidth, targetHeight, scaleExact, mediaHelper, false);
    }

    /**
     * Proxy method for other resize methods in this class. Scales the rendering image based on the supplied parameter.
     * If the target image size specifications are equal in width and height, the image will be no larger than that
     * value in any direction (height or width).  If scaleExact is true and image height or width not exactly
     * same as maxSize, then height and width will be adjusted to maxSize.
     *
     * If the target image size specifications are rectangular, the image will be scaled down and cropped to those
     * exact dimensions
     *
     * If any scaling occurs, then
     * URL is altered to include the relevant style codes.
     *
     * If image URL is empty, then returns image without resizing.     *
     * @param image image to scale
     * @param imageScaling used for ImageScalingStrategy to decide how to perform scaling, width and height are ignored in favor of target width/height
     * @param targetWidth width to scale the image to
     * @param targetHeight height to scale the image to
     * @param scaleExact
     * @param mediaHelper
     */
    public static void resizeImage(RenderImage image, ImageScaling imageScaling, int targetWidth, int targetHeight,
        boolean scaleExact, IMediaHelper mediaHelper, boolean includeHiResImage)
    {
        Validate.notNull(image, "image parameter required");
        Validate.notNull(mediaHelper, "mediaHelper parameter required");
        ImageScalingStrategy scalingStrategy = imageScaling.getScalingStrategy();

        if (ImageScalingStrategy.PRESERVE_ORIGINAL_ASPECT_RATIO == scalingStrategy)
        {
            resizeToGivenLongestSize(image, targetWidth, targetHeight, mediaHelper);
        }
        else if(ImageScalingStrategy.PRESERVE_ORIGINAL_ASPECT_RATIO_BY_HEIGHT == scalingStrategy)
        {
            resizeToHeightSize(image, targetHeight, targetWidth, mediaHelper);
        }
        else if(ImageScalingStrategy.PRESERVE_ORIGINAL_ASPECT_RATIO_BY_WIDTH == scalingStrategy)
        {
            resizeToWidthSize(image, targetWidth, mediaHelper);
        }
        else if(ImageScalingStrategy.RESIZE_TO_HEIGHT == scalingStrategy)
        {
            resizeToExactHeightSize(image, targetHeight, mediaHelper);
        }
        else if(ImageScalingStrategy.SQUARE_TOP_ALIGN == scalingStrategy)
        {
            resizeSquareTopAlign(image, targetWidth, scaleExact, mediaHelper);
        }
        else if(ImageScalingStrategy.NONE == scalingStrategy)
        {
            return;
        }
        else if(imageScaling.getWidth() == imageScaling.getHeight())
        {
            resizeImage(image, targetWidth, scaleExact, mediaHelper, includeHiResImage);
        }
        else
        {
            resizeImage(image, targetWidth, targetHeight, scaleExact, false, mediaHelper, includeHiResImage);
        }
    }

    /**
     * Scales the rendering image to the specified height and width passed in.
     *
     *
     * @param image
     * @param width
     * @param height
     * @param mediaHelper
     */
    private static void resizeToGivenLongestSize(RenderImage image, int width, int height, IMediaHelper mediaHelper)
    {
        if (image.getWidth() == null
            || image.getHeight() == null
            || image.getWidth().equals(image.getHeight())
            || image.getWidth() == 0
            || image.getHeight() == 0
            || (image.getWidth() < width && image.getHeight() < height)
            )
        {
            image.setHeight(height);
            image.setWidth(width);
        }
        else
        {
            double scaledWidth = width;
            double scaledHeight = height;
            boolean longestSideIsWidth = image.getWidth() > image.getHeight();
            double scale = 1.0;
            if (longestSideIsWidth)
            {
                scale = (double)width/(double)image.getWidth();
            }
            else
            {
                scale = (double)height/(double)image.getHeight();
            }

            scaledWidth = Math.floor(scale * image.getWidth());
            scaledHeight = Math.floor(scale * image.getHeight());

            image.setHeight((int)scaledHeight);
            image.setWidth((int)scaledWidth);
        }

        Rendering rendering = getScaleToLongestSideRendering(width > height ? width : height);
        image.setUrl(mediaHelper.appendRendering(image.getUrl(), rendering));
    }

    /**
     * Scales the rendering image to the specified height passed in. And the width will be scaled based on the ratio of
     * height.
     *
     * @param image
     * @param height
     * @param width
     * @param mediaHelper
     */
    private static void resizeToHeightSize(RenderImage image, int height, int width, IMediaHelper mediaHelper)
    {
        if (image.getWidth() == null
            || image.getHeight() == null
            || image.getWidth() == 0
            || image.getHeight() == 0)
        {
            image.setHeight(height);
            image.setWidth(width);
        }
        else
        {
            double scale = (double)height/(double)image.getHeight();
            // If there is a different ratio value, please
            int limitedWidth = width;

            //If the scaled width is greater than limited width, use resizeToGivenLongestSize strategy.
            if ((scale * image.getWidth()) > limitedWidth)
            {
                resizeImage(image, limitedWidth, height, true, false, mediaHelper);
                return;
            }

            double scaledWidth = Math.floor(scale * image.getWidth());
            double scaledHeight = height;

            image.setHeight((int)scaledHeight);
            image.setWidth((int)scaledWidth);
        }

        Rendering rendering = getScalingRendering(image, image.getWidth(), image.getHeight());
        image.setUrl(mediaHelper.appendRendering(image.getUrl(), rendering));
    }

    /**
     * Scales the rendering image to the specified width passed in. And the height will be scaled based on the ratio of
     * width.
     *
     * @param image
     * @param width
     * @param mediaHelper
     */
    private static void resizeToWidthSize(RenderImage image, int width, IMediaHelper mediaHelper)
    {
        if (image.getWidth() == null
            || image.getHeight() == null
            || image.getWidth() == 0
            || image.getHeight() == 0)
        {
            image.setHeight(width);
            image.setWidth(width);
        }
        else
        {
            double scale = (double)width/(double)image.getWidth();

            double scaledWidth = width;
            double scaledHeight = Math.floor(scale * image.getHeight());

            image.setHeight((int)scaledHeight);
            image.setWidth((int)scaledWidth);
        }

        Rendering rendering = getScalingRendering(image, image.getWidth(), image.getHeight());
        image.setUrl(mediaHelper.appendRendering(image.getUrl(), rendering));
    }

    /**
     * Scales the rendering image to the specified height passed in. 
     *
     * @param image
     * @param height
     * @param mediaHelper
     */
    static void resizeToExactHeightSize(RenderImage image, int height, IMediaHelper mediaHelper)
    {
        if (image.getWidth() == null
                || image.getHeight() == null
                || image.getWidth() == 0
                || image.getHeight() == 0)
        {
            image.setHeight(height);
            image.setWidth(height);
        }
        else
        {
            double scale = (double)height/(double)image.getHeight();
            double scaledWidth = Math.floor(scale * image.getWidth());
            double scaledHeight = height;

            image.setHeight((int)scaledHeight);
            image.setWidth((int)scaledWidth);
        }

        Rendering rendering = new Rendering().scaleToHeight(height);
        image.setUrl(mediaHelper.appendRendering(image.getUrl(), rendering));
    }


    /**
     * Scales the rendering image based on the supplied parameter.  Image will be no larger than maxSize
     * in any direction (height or width).  If scaleExact is true and image height or width not exactly
     * same as maxSize, then height and width will be adjusted to maxSize.  If any scaling occurs, then
     * URL is altered to include the relevant style codes.
     *
     * If image URL is empty, then returns image without resizing.
     *
     * Made package private for unit testing.
     *
     * @param image     the source rendering image
     * @param maxSize   the max size for the image height or width
     * @param scaleExact    if true, then image will be scaled exactly to maxSize
     * @param mediaHelper   media helper, used to rewrite url with style codes
     */
    static void resizeImage(RenderImage image, int maxSize, boolean scaleExact, IMediaHelper mediaHelper)
    {
        resizeImage(image, maxSize, scaleExact, mediaHelper, false);
    }

    /**
     * Scales the rendering image based on the supplied parameter.  Image will be no larger than maxSize
     * in any direction (height or width).  If scaleExact is true and image height or width not exactly
     * same as maxSize, then height and width will be adjusted to maxSize.  If any scaling occurs, then
     * URL is altered to include the relevant style codes.
     *
     * If image URL is empty, then returns image without resizing.
     *
     * Made package private for unit testing.
     *
     * @param image     the source rendering image
     * @param maxSize   the max size for the image height or width
     * @param scaleExact    if true, then image will be scaled exactly to maxSize
     * @param mediaHelper   media helper, used to rewrite url with style codes
     */
    static void resizeImage(RenderImage image, int maxSize, boolean scaleExact, IMediaHelper mediaHelper, boolean includeHiResImage)
    {
        Validate.notNull(image, "image parameter required");
        Validate.notNull(mediaHelper, "mediaHelper parameter required");
        Validate.isTrue(maxSize > 0, "maxSize must be greater than zero");

        if (StringUtils.isEmpty(image.getUrl()))
        {
            return;
        }
        if ((scaleExact && ((image.getHeight() == null || image.getWidth() == null) ||
                image.getHeight() != maxSize || image.getWidth() != maxSize)) ||
                (image.getHeight() == null || image.getWidth() == null ||
                image.getHeight() > maxSize || image.getWidth() > maxSize))
        {
            image.setHeight(maxSize);
            image.setWidth(maxSize);
            Rendering rendering = getScaleToSquareRendering(maxSize);
            image.setUrl(mediaHelper.appendRendering(image.getUrl(), rendering));
            if (includeHiResImage && !StringUtils.isEmpty(image.getHiresUrl()))
            {
                image.setHiresUrl(mediaHelper.appendRendering(image.getHiresUrl(), rendering));
            }
        }
    }
    
    /**
     * Generate square image for given size, but this add whitespace at the bottom if necessary.
     * @param image
     * @param maxSize
     * @param scaleExact
     * @param mediaHelper
     */
    static void resizeSquareTopAlign(RenderImage image, int maxSize, boolean scaleExact, IMediaHelper mediaHelper)
    {
        Validate.notNull(image, "image parameter required");
        Validate.notNull(mediaHelper, "mediaHelper parameter required");
        Validate.isTrue(maxSize > 0, "maxSize must be greater than zero");

        if (StringUtils.isEmpty(image.getUrl()))
        {
            return;
        }
        Integer w = image.getWidth(), h = image.getHeight();
        if ((scaleExact && ((h == null && w == null) || (h != maxSize || w != maxSize))) ||
                h > maxSize || w > maxSize)
        {
            image.setHeight(maxSize);
            image.setWidth(maxSize);
            Rendering rendering = null;
            if (h != null && w != null && w > h)
            {
                rendering = new Rendering();
                StringBuilder valuesBuilder = new StringBuilder();
                valuesBuilder.append(maxSize).append(RENDERING_VALUES_DELIMITER);
                valuesBuilder.append(maxSize).append(RENDERING_VALUES_DELIMITER);
                valuesBuilder.append(0).append(RENDERING_VALUES_DELIMITER);
                valuesBuilder.append("T");
                rendering.custom(RENDERING_SPRITE_TAG, valuesBuilder.toString());
            }
            else
            {
                rendering = getScaleToSquareRendering(maxSize);
            }
            image.setUrl(mediaHelper.appendRendering(image.getUrl(), rendering));
        }
    }
    
    /**
     * Scales the rendering image to the specified height and width passed in.
     *
     *
     * @param image
     * @param targetWidth
     * @param targetHeight
     * @param mediaHelper
     */
    private static void resizeAndCropImage(RenderImage image, int targetWidth, int targetHeight, IMediaHelper mediaHelper)
    {

        boolean scaleToWidth = (image.getHeight() != null && image.getWidth() != null && image.getHeight() > 0 &&
                image.getWidth() > 0)
                ? (targetHeight/targetWidth > image.getHeight()/image.getWidth())
                : (targetWidth > targetHeight);

        Rendering rendering = (scaleToWidth) ?
                getScaleToWidthWithCropRendering(targetWidth, targetHeight) :
                    getScaleToHeightWithCropRendering(targetWidth, targetHeight);

        image.setHeight(targetHeight);
        image.setWidth(targetWidth);
        image.setUrl(mediaHelper.appendRendering(image.getUrl(), rendering));
    }

    /**
     * Scales the rendering image to the specified height and width passed in.
     * If 'crop' is true, the image is cropped to the dimensions provided.
     * If 'crop' is false, the image is scaled to fit into the dimensions provided (without cropping the image).
     *
     * Made package private for unit testing.
     *
     * @param image
     * @param targetWidth
     * @param targetHeight
     * @param crop
     * @param mediaHelper
     */
    static void resizeImage(RenderImage image, int targetWidth, int targetHeight, boolean scaleExact, boolean crop, IMediaHelper mediaHelper)
    {
        resizeImage(image, targetWidth, targetHeight, scaleExact, crop, mediaHelper, false);
    }

    /**
     * Scales the rendering image to the specified height and width passed in.
     * If 'crop' is true, the image is cropped to the dimensions provided.
     * If 'crop' is false, the image is scaled to fit into the dimensions provided (without cropping the image).
     *
     * Made package private for unit testing.
     *
     * @param image
     * @param targetWidth
     * @param targetHeight
     * @param crop
     * @param mediaHelper
     * @param includeHiResImage
     */
    static void resizeImage(RenderImage image, int targetWidth, int targetHeight, boolean scaleExact, boolean crop, IMediaHelper mediaHelper, boolean includeHiResImage)
    {
        Validate.notNull(image, "image parameter required");
        Validate.notNull(mediaHelper, "mediaHelper parameter required");
        Validate.isTrue(targetWidth > 0 && targetHeight > 0, "Desired width and height  must be greater than zero");

        if (StringUtils.isEmpty(image.getUrl()))
        {
            return;
        }

        //Sanity Check for square images in case this method is called directly
        if(targetWidth == targetHeight)
        {
            resizeImage(image, targetWidth, scaleExact, mediaHelper, includeHiResImage);
            return;
        }

        if(crop)
        {
            resizeAndCropImage(image, targetWidth, targetHeight, mediaHelper);
            return;
        }

        Rendering rendering = getScalingRendering(image, targetWidth, targetHeight);

        image.setHeight(targetHeight);
        image.setWidth(targetWidth);
        image.setUrl(mediaHelper.appendRendering(image.getUrl(), rendering));
        if (includeHiResImage && !StringUtils.isEmpty(image.getHiresUrl()))
        {
            image.setHiresUrl(mediaHelper.appendRendering(image.getHiresUrl(), rendering));
        }
    }

    /**
     * This function picks any alternate Image Size's based on active weblabs or fancy
     * logic having to do with display properties. This function is public because the
     * image-precaching logic benefits greatly from being able to predict earlier
     * what this logic may do later.
     *
     * @param imageSize Any default image size.
     * @param product - An IProduct if available. Can be null.
     * @return An appropriate image size based on weblabs or experiments.
     */
    public static ImageSize computeImageSize(ISearchAliasProperties saDisplayProps, SearchWeblabClient searchWeblabClient, ImageSize imageSize, IProduct product)
    {
        Validate.notNull(saDisplayProps);
        Validate.notNull(searchWeblabClient);

        if (imageSize == null && product != null)
        {
            if (product.getFeaturedResult() != null)
            {
                imageSize = ImageSize.LIST_SUPERASIN;
                if (saDisplayProps.getExactMatchAsin() != null && saDisplayProps.getExactMatchAsin().getImageSize() != null)
                {
                    imageSize = saDisplayProps.getExactMatchAsin().getImageSize();
                }
            }
        }
        return imageSize;
    }
    
    /**
     * Compute image size in the feature of combine atf sprite
     * 
     * @param requestContext
     * @param namedOverride
     * @param searchAliasOverride
     * @return the image size based on request context
     */
    public static ImageSize computeImageSize(IRequestContext requestContext,
            String namedOverride, String searchAliasOverride)
    {
        IServicesProvider servicesProvider = requestContext.getServicesProvider();
        ISearchAliasProperties saDisplayProps = servicesProvider.getConfigProvider().getSearchAliasProperties(namedOverride,
                        searchAliasOverride);
        SearchWeblabClient weblabClient = servicesProvider.getSearchWeblabClient();
        ImageSize imageSize = computeImageSize(saDisplayProps, weblabClient, null, null);
        if (imageSize == null)
        {
            imageSize = saDisplayProps.getImageSize();
        }
        return imageSize;
    }

    /**
     * Get the image style codes for the pi url parameter
     *
     * This method returns null if there isn't an image url or the image url has no style codes.
     *
     * @param image The product render image
     * @return String The product render image style codes
     */
    public static String getImageStyleCodes(RenderImage image)
    {
        String imageURL = image != null ? image.getUrl() : null;
        return getImageStyleCodesByUrl(imageURL, false);
    }

    /**
     * Get the image style codes for the image hints parameter on detail page link
     * wiki page: https://w.amazon.com/index.php/MSA/HowTo/ImageStyleCodes#Basics
     * @param imageURL the image url
     * @param showDelimiter whether to show delimiter for imagestyle codes
     * @return the image style codes of product or render
     */
    public static String getImageStyleCodesByUrl(String imageURL, boolean showDelimiter)
    {
        String imageStyleCodes = null;
        if (StringUtils.isNotEmpty(imageURL))
        {
            Matcher matcher = STYLE_CODES_EXTRACTOR.matcher(imageURL);
            if (matcher.matches())
            {
                imageStyleCodes = matcher.group(1);
            }
            // add the delimiter("_") at the beginning and end
            if (StringUtils.isNotEmpty(imageStyleCodes) && showDelimiter)
            {
                imageStyleCodes = STYLE_CODES_DELIMITER + imageStyleCodes + STYLE_CODES_DELIMITER;
            }
        }
        return imageStyleCodes;
    }

    /**
     * Build the key of base64 content in the map for embed image into html
     * as the url of render image would be change in some case.
     * 
     * E.G., 
     * 1) previous url, http://ec4.images-amazon.com/images/I/41qOqHIUwVL._SL190_CR0,0,190,246_.jpg,
     * 2) after Product Data, http://ec4.images-amazon.com/images/I/41qOqHIUwVL._SL190_SY246_CR0,0,190,246_.jpg
     * 
     * So we would only use the part before the first dot.
     * It is http://ec4.images-amazon.com/images/I/41qOqHIUwVL in this example.
     * 
     * @param renderImageUrl the render image's url
     * @return the key in the map
     */
    public static String buildRenderImageKey(String renderImageUrl)
    {
        if (StringUtils.isEmpty(renderImageUrl))
        {
            return null;
        }
        
        int pos = renderImageUrl.lastIndexOf("/");
        if (pos == -1)
        {
            return renderImageUrl;
        }
        
        pos = renderImageUrl.indexOf(".", pos);
        if (pos == -1)
        {
            return renderImageUrl;
        }

        return renderImageUrl.substring(0, pos);
    }

    // gets rendering object to use.  Provides read optimized retrieval of rendering object from concurrent
    // hash map and avoids excessive object creation.  If by chance rendering object has not been initialized
    // for a given orgUnit, then it is initialized.  No big deal if concurrent threads do the initialization.
    // First one wins and others are used once and discarded.

    //Since different rendering objects will be used for different image sizes, the map considers both
    //orgUnit and ImageSize when fetching the proper rendering object.  A nested map is used because creating a
    //special key object would defeat the purpose of using a map to avoid excessive object creation.
    private static Rendering getSitbStickeringRendering(int orgUnit, ImageSize imageSize)
    {
        Integer localeKey = Integer.valueOf(orgUnit);
        Rendering rendering = null;

        if (imageSize == null)
        {
            imageSize = ImageSize.LIST;
        }

        ConcurrentHashMap<ImageSize, Rendering> sizeMap = SITB_STICKERING_RENDERING_MAP.get(localeKey);

        if (sizeMap != null)
        {
            rendering = sizeMap.get(imageSize);

            if (rendering != null)
            {
                return rendering;
            }
        }
        else
        {
            sizeMap = new ConcurrentHashMap<ImageSize, Rendering>(ImageSize.values().length);
            SITB_STICKERING_RENDERING_MAP.putIfAbsent(localeKey, sizeMap);
        }

        rendering = new Rendering().scaleToLongest(Math.max(imageSize.getHeight(), imageSize.getWidth()))
                .stickerGeneric("sitb-sticker-arrow-dp", StickerLocation.TopRight, 12, -18).sharpen(30);
        rendering.orgUnit(orgUnit);
        sizeMap.putIfAbsent(imageSize, rendering);

        return rendering;
    }

    // gets rendering object to use.  Provides read optimized retrieval of rendering object from concurrent
    // hash map and avoids excessive object creation.  If by chance rendering object has not been initialized
    // for a given size, then it is initialized.  No big deal if concurrent threads do the initialization.
    // First one wins and others are used once and discarded.
    private static Rendering getScaleToSquareRendering(int size)
    {
        Integer key = Integer.valueOf(size);
        Rendering rendering;
        rendering = SQUARE_RENDERING_MAP.get(key);
        if (rendering != null)
        {
            return rendering;
        }

        // need to initialize rendering
        rendering = new Rendering().scaleToSquare(size);
        SQUARE_RENDERING_MAP.putIfAbsent(key, rendering);

        return rendering;
    }

    // Scales image to the longest side while retaining its original aspect ratio
    //
    // gets rendering object to use.  Provides read optimized retrieval of rendering object from concurrent
    // hash map and avoids excessive object creation.  If by chance rendering object has not been initialized
    // for a given size, then it is initialized.  No big deal if concurrent threads do the initialization.
    // First one wins and others are used once and discarded.
    private static Rendering getScaleToLongestSideRendering(int size)
    {
        Integer key = Integer.valueOf(size);
        Rendering rendering = SCALE_TO_LONGEST_SIDE_MAP.get(key);
        if (rendering != null)
        {
            return rendering;
        }

        // need to initialize rendering
        rendering = new Rendering().scaleToLongest(size);
        SCALE_TO_LONGEST_SIDE_MAP.putIfAbsent(key, rendering);

        return rendering;
    }

    //
    // Scales the image to the specified values, adding whitespace if the image is too small to scale.
    //
    // gets rendering object to use.  Provides read optimized retrieval of rendering object from concurrent
    // hash map and avoids excessive object creation.  If by chance rendering object has not been initialized
    // for a given size, then it is initialized.  No big deal if concurrent threads do the initialization.
    // First one wins and others are used once and discarded.
    private static Rendering getScalingRendering(RenderImage image, int width, int height)
    {
        int imageHeight = image.getHeight() == null ? 0 : image.getHeight().intValue();
        int imageWidth = image.getWidth() == null ? 0 : image.getWidth().intValue();

        Integer key = convertToIntegerKey(width, height);
        Rendering rendering;

        // Scale a 'square' image
        if(imageHeight == imageWidth)
        {
            rendering = RECTANGULAR_SCALING_RENDERING_MAP_FOR_SQUARE.get(key);

            if(rendering == null)
            {
                rendering = new Rendering().scaleToLongest(Math.min(width, height));
                rendering.crop(0, 0, width, height);
                RECTANGULAR_SCALING_RENDERING_MAP_FOR_SQUARE.putIfAbsent(key, rendering);
            }
            return rendering;
        }

        // Scale 'fat' image.
        if(imageWidth  > imageHeight)
        {
            rendering = RECTANGULAR_SCALING_RENDERING_MAP_BY_WIDTH.get(key);

            if(rendering == null)
            {
                rendering = new Rendering().scaleToLongest(width);
                rendering.scaleToHeight(height);
                rendering.crop(0, 0, width, height);
                RECTANGULAR_SCALING_RENDERING_MAP_BY_WIDTH.putIfAbsent(key, rendering);
            }
            return rendering;
        }

        // Default. Scale a 'tall' image.
        rendering = RECTANGULAR_SCALING_RENDERING_MAP_BY_HEIGHT.get(key);
        if(rendering == null)
        {
            rendering = new Rendering().scaleToLongest(height);
            rendering.scaleToWidth(width);
            rendering.crop(0, 0, width, height);
            RECTANGULAR_SCALING_RENDERING_MAP_BY_HEIGHT.putIfAbsent(key, rendering);
        }

        return rendering;
    }


    // gets rendering object to use.  Provides read optimized retrieval of rendering object from concurrent
    // hash map and avoids excessive object creation.  If by chance rendering object has not been initialized
    // for a given size, then it is initialized.  No big deal if concurrent threads do the initialization.
    // First one wins and others are used once and discarded.
    private static Rendering getScaleToWidthWithCropRendering(int width, int height)
    {
        Integer key = convertToIntegerKey(width, height);
        Rendering rendering;
        rendering = SCALE_TO_WIDTH_CROP_MAP.get(key);
        if (rendering != null)
        {
            return rendering;
        }

        // need to initialize rendering
        rendering = new Rendering().scaleToWidth(width);
        rendering.crop(0, 0, width, height);
        SCALE_TO_WIDTH_CROP_MAP.putIfAbsent(key, rendering);

        return rendering;
    }

    // gets rendering object to use.  Provides read optimized retrieval of rendering object from concurrent
    // hash map and avoids excessive object creation.  If by chance rendering object has not been initialized
    // for a given size, then it is initialized.  No big deal if concurrent threads do the initialization.
    // First one wins and others are used once and discarded.
    private static Rendering getScaleToHeightWithCropRendering(int width, int height)
    {
        Integer key = convertToIntegerKey(width, height);
        Rendering rendering;
        rendering = SCALE_TO_HEIGHT_CROP_MAP.get(key);
        if (rendering != null)
        {
            return rendering;
        }

        // need to initialize rendering
        rendering = new Rendering().scaleToHeight(height);
        rendering.crop(0, 0, width, height);
        SCALE_TO_HEIGHT_CROP_MAP.putIfAbsent(key, rendering);

        return rendering;
    }

    // we expect small numbers; shift width to upper bytes, then or the two values to create
    // an Integer to be used as a key in our map
    private static Integer convertToIntegerKey(int width, int height)
    {
        return width << 16 | height;
    }

    /**
     * Apply sash to an image.
     * @param image to apply sticker
     * @param product IProduct
     * @param mediaHelper IMediaHelper
     */
    public static void applyPrimeSash(RenderImage image, int orgUnit, IProduct product, IMediaHelper mediaHelper)
    {
        // Do not apply sash to placeholder
        if (image.isPlaceholderImage())
        {
            return;
        }

        Rendering rendering = new Rendering();
        int w = image.getWidth() == null ? 0 : image.getWidth().intValue();

        if (w < SCALE_SIZE)
        {
            rendering.upscaleToWidth(SCALE_SIZE);
        }
        else
        {
            rendering.scaleToWidth(SCALE_SIZE);
        }

        rendering.stickerGeneric(PRIME_SASH, StickerLocation.TopLeft, 0, 0);

        // Apply Japanese sash for JP
        if(Marketplace.getOrgUnit(Locale.JAPAN) == orgUnit)
        {
            rendering.orgUnit(orgUnit);
        }

        image.setUrl(mediaHelper.appendRendering(image.getUrl(), rendering));
        image.setWidth(Integer.valueOf(SCALE_SIZE));
        image.setHeight(Integer.valueOf(SCALE_SIZE));
    }

    /**
     * check whether all product results from media service are present.
     * 
     * @param resultBatch the result batch from media service
     * @return true if all results are present
     *         false if not all results present
     */
    public static boolean isAllProductResultPresent(ResultBatch<ProductResult> resultBatch)
    {
        if (resultBatch == null || resultBatch.size() == 0)
        {
            return false;
        }
        
        boolean valid = true;
        try
        {
            int size = resultBatch.size();
            for (int i = 0; i < size; i++)
            {
                ProductResult prodResult = resultBatch.get(i);
                if (!prodResult.isPresent())
                {
                    valid = false;
                    break;
                }
            }
        }
        catch(MediaIOException exception)
        {
            LOGGER.error("error in get element from result batch by media client in atf image sprite."
                    + exception.getMessage());
            valid = false;
        }
        return valid;
    }
    
    /**
     * get the raw url of image. E.g., the current image url is
     * (http://ec4.images-amazon.com/images/I/41Nd-aBlcNL._SL190_CR0,0,190,246_.jpg)
     * and the raw url would be "http://ec4.images-amazon.com/images/I/41Nd-aBlcNL.jpg"
     * 
     * @param imageUrl the image url after calculation
     * @return the raw url of image just having physical id
     */
    public static String getImageRawUrl(String imageUrl)
    {
        if (imageUrl == null)
        {
            return null;
        }
        
        int indexOfLastSlash = imageUrl.lastIndexOf('/');
        int indexOfFirstDot = imageUrl.indexOf('.', indexOfLastSlash);
        int indexOfLastDot = imageUrl.lastIndexOf('.');
        
        String imageRawUrl = null;
        if (indexOfFirstDot != -1 && indexOfLastDot != -1)
        {
            String removed = imageUrl.substring(indexOfFirstDot, indexOfLastDot);
            imageRawUrl = imageUrl.replaceFirst(removed, "");
        }
        
        return imageRawUrl;
    }

    /**
     * Autocrop product image with a tag
     *
     * @param image
     * @param mediaHelper
     */
    public static void autocropImage(RenderImage image, IMediaHelper mediaHelper)
    {
        autocropImage(image, mediaHelper, false);
    }

    /**
     * Autocrop product image with a tag
     *
     * @param image
     * @param mediaHelper
     * @param includeHiResImage
     */
    public static void autocropImage(RenderImage image, IMediaHelper mediaHelper, boolean includeHiResImage)
    {
        Rendering autocropRendering = new Rendering();
        autocropRendering.custom(RENDERING_AUTO_CROP_TAG, "");
        image.setUrl(mediaHelper.prependRendering(image.getUrl(), autocropRendering.toString()));
        if (includeHiResImage && !StringUtils.isEmpty(image.getHiresUrl()))
        {
            image.setHiresUrl(mediaHelper.prependRendering(image.getHiresUrl(), autocropRendering.toString()));
        }
    }

    /**
     * Finalize product image tags
     */
    public static void finalizeProductImage(RenderImage image, IProduct product, IRequestContext requestContext,
                                            IPropertyAccessor propertyAccessor)
    {
        applyCompressionSettings(requestContext, product, image, propertyAccessor, false);
    }

    /**
     * Finalize product image tags
     */
    public static void finalizeProductImage(RenderImage image, IProduct product, IRequestContext requestContext,
        IPropertyAccessor propertyAccessor, boolean includeHiResImage)
    {
        applyCompressionSettings(requestContext, product, image, propertyAccessor, includeHiResImage);
    }

    /**
     * Apply image compression settings - quality level and webP
     */
    private static void applyCompressionSettings(IRequestContext requestContext, IProduct product, RenderImage image,
        IPropertyAccessor propertyAccessor, boolean includeHiResImage)
    {
        if (image == null || requestContext == null ||
            requestContext.getSearchRequest() == null || requestContext.getServicesProvider() == null ||
            requestContext.getServicesProvider().getMediaHelper() == null ||
            propertyAccessor == null) {
            LOGGER.error("Invalid parameters in finalizeProductImage");
            return;
        }

        if (image.getUrl() == null) {
            LOGGER.warn("Image URL is null");
            return;
        }

        if (image.isPlaceholderImage()) {
            // We shouldn't scale placeholder images since they are text heavy and look really bad compressed.
            return;
        }

        if (product != null && (product.isHeroAsin() || product.getFeaturedResult() != null)) {
            return;
        }

        boolean useWebp = false;
        int quality = 0;

        // Set compression level for high res images

        if (image.isHighDensity()) {
            quality = HIGH_DENSITY_IMAGE_COMPRESSION_RATIO;
            useWebp = requestContext.getSearchRequest().acceptsWebp();
        }
        else {
            if (requestContext.getSearchRequest().acceptsWebp()) {
                quality = propertyAccessor.getInt("webpQualityLevel");

                if (quality > 0)
                {
                    useWebp = true;
                }
            }

            if (!useWebp)
            {
                quality = propertyAccessor.getInt("jpegQualityLevel");
            }
        }

        if (!useWebp && quality < 1) {
            return;
        }

        Rendering rendering = new Rendering();

        if (useWebp) {
            rendering.custom("FM", "webp");
        }

        if (quality > 0) {
            rendering.custom("QL", String.valueOf(quality));
        }

        image.setUrl(requestContext.getServicesProvider().getMediaHelper().appendRendering(image.getUrl(), rendering));
        if (includeHiResImage && !StringUtils.isEmpty(image.getHiresUrl()))
        {
            image.setHiresUrl(requestContext.getServicesProvider().getMediaHelper().appendRendering(image.getHiresUrl(), rendering));
        }
    }

    /**
     * Gets the URL for prefetch images
     * 
     * @param url inband image url
     * @param requestContext
     * @return url of inband image image with updated MSA tags
     */
    public static String getPrefetchImageUrl(String url, IRequestContext requestContext)
    {
        SearchRequest sr = requestContext.getSearchRequest();
        if (InbandImageUtil.inbandImageEnabled(requestContext) && sr.getImageWidth() != null && sr.getImageHeight() != null)
        {
            return MSAUtils.updateImageTags(url, Math.max(sr.getImageWidth(), sr.getImageHeight()),
                    sr.getImageAutoCrop());
        }
        if (requestContext.getRequestType() != null && requestContext.getRequestType().isDataRequest()
                && sr.getImageResolution() != null)
        {
            return MSAUtils.updateImageTags(url, sr.getImageResolution(), sr.getImageAutoCrop());
        }
        return url;
    }

    /**
     * Build high res images
     */
    public static void buildScreenImages(IRequestContext requestContext, ISearchAliasProperties saDisplayProps,
        IPropertyAccessor propertyAccessor, IMediaHelper mediaHelper, IProduct product, RenderImage sourceImage,
        RenderImage standardImage, ImageScaling imageScaling, boolean scaleExact, boolean includeHiResImage)
    {
        List<ImageSizeDensity> sizes = HighResUtil.getHighResSizes(requestContext, sourceImage.getWidth(),
            sourceImage.getHeight(), standardImage.getWidth(), standardImage.getHeight());

        List<RenderImage> images = new ArrayList<>();

        for (ImageSizeDensity size : sizes)
        {
            RenderImage image = new RenderImage(sourceImage);
            image.setPixelDensity(size.getPixelDensity());

            ImageScaling scaling;
            
            if (propertyAccessor.getBoolean("useResponsiveImageContainer")) {
                // For weblab SEARCH_92346:T2, We want to resize images based on an aspect ratio
                double aspectRatio = 1.5;
                int scalingHeight = (int)(size.getWidth() * aspectRatio);
                scaling = new ImageScaling(size.getWidth(), scalingHeight,
                        imageScaling.getScalingStrategy());
            }
            else {
                scaling = new ImageScaling(size.getWidth(), size.getHeight(),
                        imageScaling.getScalingStrategy());
            }

            renderBaseProductImage(image, scaling, scaleExact, mediaHelper, product, requestContext, saDisplayProps,
                propertyAccessor, includeHiResImage);

            images.add(image);
        }
        if (propertyAccessor.getBoolean("useResponsiveImages") || propertyAccessor.getBoolean("useHighResImages"))
        {
            HighResUtil.updateImage(requestContext, standardImage, images, true, true);
        }
        else {
            HighResUtil.updateImage(requestContext, standardImage, images, false, false);
        }
    }

    /**
     * Get physical ID from image URL.
     *
     * @param imageUrl Image URL
     * @return physical ID
     */
    private static String getPhysicalID(String imageUrl)
    {
        if (imageUrl == null)
        {
            return null;
        }
        int indexOfLastSlash = imageUrl.lastIndexOf('/');
        int indexOfFirstDot = imageUrl.indexOf('.', indexOfLastSlash);

        return indexOfLastSlash != -1 && indexOfFirstDot != -1 ?
                imageUrl.substring(indexOfLastSlash + 1, indexOfFirstDot) : null;
    }

    /**
     * Check the URL is supported for sprites. Only support "images/I/", not support "images/G/" or "images/S/".
     * For current logic, it only support jpg, but not support PNG image.
     *
     * More details, see at https://w.amazon.com/index.php/MSA/HowTo/ImageStyleCodes#Product_Image_Sprites
     *
     * @param imageUrl target image url
     * @return true for support.
     */
    private static boolean isSupportedSpritesUrl(String imageUrl)
    {
        return StringUtils.isNotBlank(imageUrl) && imageUrl.contains("images/I/") && imageUrl.length() > 4
                && imageUrl.substring(imageUrl.length() - 4, imageUrl.length()).toLowerCase().equals(".jpg");
    }

    /**
     * Validation for RGB value.
     *
     * @param value value of RGB
     * @return true to valid.
     */
    private static boolean isValidRGBValue(int value)
    {
        return 0 <= value && value <= 255;
    }

    /**
     * Build sprite images for a group of RenderImages.
     *
     * Support:
     * (1) Width
     * (2) Height
     * (3) Padding, not set, equals to 0
     * (4) Alignment, set to C
     * (5) Background RGB, not set, equals to (255, 255, 255)
     *
     * Default quality is set to be ImageUtil.HIGH_DENSITY_IMAGE_COMPRESSION_RATIO.
     *
     * @param renderImages origin render images
     * @param imageSize    ImageSize
     * @param mediaHelper  padding between images
     * @return List of RenderSpriteImage, any item in it might be null.
     */
    public static List<RenderSpriteImage> buildSpriteImages(List<RenderImage> renderImages, ImageSize imageSize,
            IMediaHelper mediaHelper)
    {
        return buildSpriteImages(renderImages, imageSize, 0, 'C', -1, -1, -1, HIGH_DENSITY_IMAGE_COMPRESSION_RATIO
                , mediaHelper);
    }

    /**
     * Build sprite images for a group of RenderImages.
     *
     * Support:
     * (1) Width
     * (2) Height
     * (3) Padding
     * (4) Alignment
     * (5) Background RGB
     *
     * More details, see at https://w.amazon.com/index.php/MSA/HowTo/ImageStyleCodes#Product_Image_Sprites
     *
     * @param renderImages origin render images
     * @param imageSize    ImageSize
     * @param padding      padding between images
     * @param alignment    'T' for top, 'C' for center, 'B' for bottom
     * @param R            RGB Red
     * @param G            RGB Green
     * @param B            RGB Blue
     * @param quality      quality of image
     * @param mediaHelper  MediaHelper
     * @return List of RenderSpriteImage, any item in it might be null.
     */
    public static List<RenderSpriteImage> buildSpriteImages(List<RenderImage> renderImages, ImageSize imageSize,
            int padding, char alignment, int R, int G, int B, int quality, IMediaHelper mediaHelper)
    {
        if (CollectionUtils.isEmpty(renderImages))
        {
            return ListUtils.EMPTY_LIST;
        }
        // Mapping physical id to renderImage (validate)
        List<String> urls = renderImages.stream()
                .filter(Objects::nonNull)
                .map(RenderImage::getUrl)
                .filter(ImageUtil::isSupportedSpritesUrl)
                .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(urls) && urls.size() > 1)
        {
            // choose base raw url
            String baseUrl = getImageRawUrl(urls.get(0));
            String baseUrlPhysicalID = getPhysicalID(urls.get(0));

            // generate rendering
            List<String> physicalIDs = urls.stream().distinct()
                    .map(ImageUtil::getPhysicalID).skip(1)
                    .collect(Collectors.toList());

            boolean isValidPadding = padding > 0;
            boolean isValidAlignment = (alignment == 'T' || alignment == 'C' || alignment == 'B');
            boolean isValidRGB = isValidRGBValue(R) && isValidRGBValue(G) && isValidRGBValue(B);

            // scaling
            StringBuilder commands = new StringBuilder().append("_SP");
            if (imageSize.getHeight() == imageSize.getWidth() && !isValidPadding && !isValidAlignment && !isValidRGB)
            {
                commands.append(imageSize.getWidth());
            }
            else
            {
                commands.append(imageSize.getWidth());
                commands.append(',');
                commands.append(imageSize.getHeight());
            }

            // padding
            if (isValidPadding)
            {
                commands.append(',').append(padding);
            }
            else if (isValidAlignment || isValidRGB)
            {
                commands.append(",0");
            }

            // alignment
            if (isValidAlignment)
            {
                commands.append(',').append(alignment);
            }
            else if (isValidRGB)
            {
                commands.append(",C");
            }

            // background RGB
            if (isValidRGB)
            {
                commands.append(',').append(R).append(',').append(G).append(',').append(B);
            }

            // physicalID
            commands.append("|");
            physicalIDs.forEach(physicalID -> commands.append(physicalID).append(".jpg,"));
            if (commands.charAt(commands.length() - 1) == ',')
            {
                commands.deleteCharAt(commands.length() - 1);
            }
            physicalIDs.add(0, baseUrlPhysicalID);

            if (quality > 0)
            {
                commands.append("_QL").append(String.valueOf(quality));
            }

            String spriteUrl = mediaHelper.appendRendering(baseUrl, commands.toString());
            Map<String, RenderSpriteImage> map = IntStream.range(0, physicalIDs.size())
                    .mapToObj(index -> index)
                    .collect(Collectors.toMap(physicalIDs::get, index ->
                    {
                        RenderSpriteImage image = new RenderSpriteImage(spriteUrl, StringUtils.EMPTY,
                                imageSize.getHeight(), imageSize.getWidth() * physicalIDs.size());
                        image.setXOffset(index * (imageSize.getWidth() + padding));
                        image.setYOffset(0);
                        return image;
                    }, (o, n) -> o));
            return IntStream.range(0, renderImages.size())
                    .mapToObj(index ->
                    {
                        RenderImage renderImage = renderImages.get(index);
                        if (renderImage == null)
                        {
                            return null;
                        }
                        String physicalID = getPhysicalID(renderImage.getUrl());
                        RenderSpriteImage renderSpriteImage = map.get(physicalID);
                        if (renderSpriteImage != null)
                        {
                            renderSpriteImage.setAltText(renderImage.getAltText());
                            return renderSpriteImage;
                        }
                        else
                        {
                            return null;
                        }
                    })
                    .collect(Collectors.toList());
        }
        else
        {
            return ListUtils.EMPTY_LIST;
        }
    }
}