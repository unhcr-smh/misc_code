import cv2   #pip install opencv-python
import pytesseract
import numpy as np

img = cv2.imread('E:/_UNHCR/CODE/kobo_gen_survey/download_survey_pics/output_controller/media_file=fifield%2Fattachments%2Fb0b534743414482c9addd247d3ca241e%2F0f0e907d-9ecd-41b7-9e9a-cb08b6674e6e%2FWhatsApp_Image_2024-02-20_at_14.27.49_8354787b-15_21_28.jpg')#'E:/_UNHCR/CODE/kobo_gen_survey/ocr/tmp/x320_image1.jpeg')
gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY) 
print('gray')
blur = cv2.GaussianBlur(gray, (5,5), 0) 
print('blur')
# Use adaptive thresholding to convert the image to binary 
# ADAPTIVE_THRESH_GAUSSIAN_C: threshold value is the weighted sum of 
# neighbourhood values where weights are a gaussian window. 
# BLOCK Size - It decides the size of neighbourhood area. 
# C - It is just a constant which is subtracted from the mean or weighted 
# mean calculated. 
bin_img = cv2.adaptiveThreshold(blur, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 31, 2)
print('bin')
# Perform dilation and erosion to remove some noise 
kernel = np.ones((1, 1), np.uint8) 
print('kernal')
img = cv2.dilate(bin_img, kernel, iterations=1) 
print('dilate')
img = cv2.erode(img, kernel, iterations=1) 


# Adding custom options
custom_config = r'--oem 3 --psm 6'
res = pytesseract.image_to_string(img, config=custom_config)

print('XXXXXX',res,'YYYYYY')