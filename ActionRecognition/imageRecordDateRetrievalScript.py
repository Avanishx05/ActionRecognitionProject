from PIL import Image
from PIL.ExifTags import TAGS


def get_exif_data(image_path):
    image = Image.open(image_path)
    exif_data = image._getexif()
    if not exif_data:
        return None

    exif_info = {}
    for tag, value in exif_data.items():
        tag_name = TAGS.get(tag, tag)
        exif_info[tag_name] = value

    return exif_info


def get_datetime_original(image_path):
    exif_info = get_exif_data(image_path)
    if exif_info is None:
        return "No EXIF data found"

    date_time_original = exif_info.get('DateTimeOriginal')
    if date_time_original:
        return f"DateTimeOriginal: {date_time_original}"
    else:
        return "DateTimeOriginal not found in EXIF data"


# Example usage
image_path = 'testimg2.jpeg'
print(get_datetime_original(image_path))



