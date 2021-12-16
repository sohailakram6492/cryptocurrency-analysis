import cv2
import face_recognition
import os

class RealTimeRecognition():
    def __init__(self):
        # face_detection model file
        self.face_cascade = cv2.CascadeClassifier(
            'FaceRecognition/model/haarcascade_frontalface_default.xml')
        # all images
        self.images_list = os.listdir(r'FaceRecognition/Images/')
        # store all encodings of Images dir
        self.all_encodings = {}
        self.encoding()

    def encoding(self):
        for i in self.images_list:
            image = face_recognition.load_image_file(fr'FaceRecognition/Images/{i}')
            data=face_recognition.face_encodings(image)
            if data:
                self.all_encodings[i.split('.')[0]]=data[0]
        
    # comparing images
    def comparison(self,img_matrix):
        data=face_recognition.face_encodings(img_matrix)
        if not data:
            return ''
        for k,v in self.all_encodings.items():
            results = face_recognition.compare_faces(
                [v], data[0], tolerance=0.45)
            if results[0]:
                return k
                
    # cv2 work
    def open_camer(self):
        cam=cv2.VideoCapture(0)
        counter=20
        name= ''
        
        while True:
            ret,frame=cam.read()
            if not ret or cv2.waitKey(1)==27:# press esc for exit.
                break
            gray = cv2.cvtColor(frame,cv2.COLOR_BGR2GRAY)
            box,detections = self.face_cascade.detectMultiScale2(gray,minNeighbors=8)
            if len(detections) > 0 and detections[0] >= 20:
                x,y,w,h= box[0]
                cv2.rectangle(frame,(x,y),(x+w,y+h),(0,255,0),2)
                cv2.putText(frame, str(detections[0]), (0,50), cv2.FONT_HERSHEY_SIMPLEX, 2, [0, 255, 0], 2)
            if counter <20:
                counter+=1
                if name:
                    cv2.putText(frame,name, (x,y-30), cv2.FONT_HERSHEY_SIMPLEX, 2, [0, 255, 0], 2)
            else:
                name=''
            if len(detections) > 0 and detections[0] >= 30 and counter%20==0:
                counter =0
                x,y,w,h= box[0]
                txt=self.comparison(frame)
                if txt:
                    print(txt)
                    name=txt
                    cv2.putText(frame,txt, (x,y-30), cv2.FONT_HERSHEY_SIMPLEX, 2, [0, 255, 0], 2)
            # cv2.imshow('',frame)
            if not ret:
                break
            else:
                ret, buffer = cv2.imencode(".jpg", frame)
                frame = buffer.tobytes()
            yield (b"--frame\r\n" b"Content-Type: image/jpeg\r\n\r\n" + frame + b"\r\n")

# RealTimeRecognition().open_camer()