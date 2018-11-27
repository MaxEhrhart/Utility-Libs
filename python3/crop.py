from PIL import Image

#def crop(image_path, coords, saved_location):
#    """
#    @param image_path: The path to the image to edit
#    @param coords: A tuple of x/y coordinates (x1, y1, x2, y2)
#    @param saved_location: Path to save the cropped image
#    """
#    image_obj = Image.open(image_path)
#    cropped_image = image_obj.crop(coords)
#    #cropped_image.save(saved_location)
#    cropped_image.show()

im = Image.open(r"C:\Users\maximilian.erhard\Desktop\images.jpeg")
width, height = im.size
numberOfSplits = 3
splitDist = height / numberOfSplits #how many pixels each crop should be in width
print(width, height) #prints 1180, 842

for i in range(0, numberOfSplits):
    # vertical
    x1 = splitDist * i
    y1 = 0
    x2 = splitDist+x1
    y2 = height+y1

    # horizontal
    #x1 = 
    #x2 = 
    #y1 = 
    #y2 = 
    regiao = (x1, y1, x2, y2)

    croppedImg = im.crop(regiao)
    croppedImg.save("new-img" + str(i) + ".png") #save to file


