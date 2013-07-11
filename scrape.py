from embedly import Embedly
import tme

key = "645c9cbba816467c8bc0f42be1782bba"

client = Embedly(key)

obj = client.extract('http://www.sap.com/corporate-en/press-and-media/newsroom/press.epx?PressID=21209')

print(obj.keys())
print(obj['title'])

print("=" * len(obj['title']))

print(obj['content'])
print("create engine")

text = obj['content']
engine = tme.Engine()
print(text)
analyzer = engine.compile_analyzer(enable_phrases=True, annotations=["http://www.attensity.net/entity#Person"])
for anno in analyzer(text=text, options={'D:input-document-type': 'html'}):
    substr = text[anno.beginOffset:anno.endOffset]
    print("@({1.beginOffset}-{1.endOffset}) {1.typeUri} {0} {1.attributes}".format(repr(substr), anno))



