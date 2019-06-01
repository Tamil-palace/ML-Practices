import cv2,os,re,sys
import numpy as np
from skimage.measure import compare_ssim as ssim
import xlwt

wb = xlwt.Workbook()
ws = wb.add_sheet('images')

print(os.listdir(r"D:\\OCR\\06\\14\Images\\"))
incr=0
for index,images in enumerate(os.listdir(r"D:\OCR\06\29\Newimages\\")):
    # print(re.match(r".*\.jpg",images,re.I))
    if re.match(r".*\.jpg",images,re.I):
        print(images)
        for index1,images1 in enumerate(os.listdir(r"D:\OCR\06\29\Oldimages\\")):
            print("index = "+str(index),"index1 = "+str(index1))
            # if index>index1:
            #     continue
            print(index, index1)
            if re.match(r".*\.jpg", images, re.I):
                try:
                    x1 = cv2.imread("D:\\OCR\\06\\29\\Newimages\\"+images)
                    x2 = cv2.imread("D:\\OCR\\06\\29\\Oldimages\\" + images1)
                    # print(tuple(x2.shape[0],x2.shape[1]))
                    # print(x2)
                    if x2 is not None:
                        # shape=()
                        # shape=x2.shape[0], x2.shape[1]
                        # print(shape)
                        if x2.shape[1]>x2.shape[0]:
                            shape=x2.shape[1]
                        else:
                            shape = x2.shape[0]
                        x1 = cv2.resize(x1,(shape,shape))
                        x2 = cv2.resize(x2,(shape,shape))
                        # print(x1.shape)
                        # print(x2.shape)
                        # input()
                        x1 = cv2.cvtColor(x1, cv2.COLOR_BGR2GRAY)
                        x2 = cv2.cvtColor(x2, cv2.COLOR_BGR2GRAY)
                        # shape = ()
                        # shape = x2.shape[0], x2.shape[1]
                        # x1 = cv2.resize(x1, shape)
                        s = ssim(x1,x2)
                        with open("Image_comparision.txt","a") as fh:
                            fh.write(str(images).replace("SLASH","/")+"\t"+str(images1).replace("SLASH","/")+"\t"+str(round(s,5))+"\n")
                        ws.write(int(incr)+1,0, str(images).replace("SLASH","/"))
                        ws.write(int(incr)+1,1, str(images1).replace("SLASH","/"))
                        ws.write(int(incr)+1,2,round(s,5))
                        incr+=1
                        print(round(s,5))
                except Exception as e:
                    print(e)
                    pass
                    # print(x1.shape)
                    # print(x2.shape)
                    # print(str(sys.exc_info()[-1].tb_lineno))
                    # input()
                    # print(x1.shape)
                    # print(x2.shape)
                    # x2 = cv2.resize(x1, x2.shape)
                    # print(x1.shape)
                    # print(x2.shape)
                    # # x1 = cv2.imread("D:\\OCR\\06\\14\\Images\\Testing\\" + images)
                    # # x2 = cv2.imread("D:\\OCR\\06\\14\\Images\\Testing\\" + images1)
                    # x1 = cv2.cvtColor(x1, cv2.COLOR_BGR2GRAY)
                    # x2 = cv2.cvtColor(x2, cv2.COLOR_BGR2GRAY)
                    # #
                    # # # absdiff = cv2.absdiff(x1, x2)
                    # # # cv2.imwrite("absdiff.png", absdiff)
                    # #
                    # # # diff = cv2.subtract(x1, x2)
                    # # # print(diff)
                    # # # result = not np.any(diff)
                    # s = ssim(x1, x2)
                    # ws.write(0, index1 + 1, images)
                    # ws.write(1, index1 + 1, images1)
                    # ws.write(2, index1 + 1, round(s, 5))
                    # print(round(s, 5))
                    # input()


wb.save('Images_fulliteration.xls')
