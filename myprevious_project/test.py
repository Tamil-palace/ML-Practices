# import ftfy,html,re
# from fuzzywuzzy import fuzz
# print(ftfy.fix_text("sac \xc3\xa0 dos pratique \xc3\xa0 linge, sac \xc3\xa0 linge (55,9\xc2\xa0x 71,1\xc2\xa0cm), vert, 22x28"))
# # lst=["123123","1","2","3","4","sdf"]
# # lst1=["1","6","7","8"]
# # s=lst+lst1
# # print(s)
# #
# itemloop="""This XML file does not appear to have any style information associated with it. The document tree is shown below.
# <ItemLookupResponse xmlns="http://webservices.amazon.com/AWSECommerceService/2011-04-01">
# <OperationRequest>
# <HTTPHeaders>
# <Header Name="UserAgent" Value="Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36"/>
# </HTTPHeaders>
# <RequestId>790999d3-e8fb-44a4-b3e4-4a7400172490</RequestId>
# <Arguments>
# <Argument Name="AWSAccessKeyId" Value="AKIAI5WV5JKODYZMIJJA"/>
# <Argument Name="AssociateTag" Value="amazon00f-20"/>
# <Argument Name="IncludeProtectedContent" Value="true"/>
# <Argument Name="ItemId" Value="B01E9I4D0G"/>
# <Argument Name="MerchantId" Value="All"/>
# <Argument Name="Operation" Value="ItemLookup"/>
# <Argument Name="ResponseGroup" Value="ItemAttributes,Images"/>
# <Argument Name="Service" Value="AWSECommerceService"/>
# <Argument Name="Timestamp" Value="2018-05-01T08:08:28Z"/>
# <Argument Name="Version" Value="2011-04-01"/>
# <Argument Name="Signature" Value="3OmwEtUBGWG8cqy16/QnSAO0KUbXsua2Z5PIrPmphNA="/>
# </Arguments>
# <RequestProcessingTime>0.0090828350000000</RequestProcessingTime>
# </OperationRequest>
# <Items>
# <Request>
# <IsValid>True</IsValid>
# <ItemLookupRequest>
# <Condition>New</Condition>
# <DeliveryMethod>Ship</DeliveryMethod>
# <IdType>ASIN</IdType>
# <MerchantId>All</MerchantId>
# <OfferPage>1</OfferPage>
# <ItemId>B01E9I4D0G</ItemId>
# <ResponseGroup>ItemAttributes</ResponseGroup>
# <ResponseGroup>Images</ResponseGroup>
# <ReviewPage>1</ReviewPage>
# <ReviewSort>-HelpfulVotes</ReviewSort>
# <VariationPage>All</VariationPage>
# </ItemLookupRequest>
# </Request>
# <Item>
# <ASIN>B01E9I4D0G</ASIN>
# <ParentASIN>B01E9IWJ0M</ParentASIN>
# <DetailPageURL>
# https://www.amazon.de/kompatibel-Laserjet-CM1312nf-CM1312nfi-CM1415FNW/dp/B01E9I4D0G?psc=1&SubscriptionId=AKIAI5WV5JKODYZMIJJA&tag=amazon00f-20&linkCode=xm2&camp=2025&creative=165953&creativeASIN=B01E9I4D0G
# </DetailPageURL>
# <ItemLinks>
# <ItemLink>
# <Description>Add To Wishlist</Description>
# <URL>
# https://www.amazon.de/gp/registry/wishlist/add-item.html?asin.0=B01E9I4D0G&SubscriptionId=AKIAI5WV5JKODYZMIJJA&tag=amazon00f-20&linkCode=xm2&camp=2025&creative=12738&creativeASIN=B01E9I4D0G
# </URL>
# </ItemLink>
# <ItemLink>
# <Description>Tell A Friend</Description>
# <URL>
# https://www.amazon.de/gp/pdp/taf/B01E9I4D0G?SubscriptionId=AKIAI5WV5JKODYZMIJJA&tag=amazon00f-20&linkCode=xm2&camp=2025&creative=12738&creativeASIN=B01E9I4D0G
# </URL>
# </ItemLink>
# <ItemLink>
# <Description>All Customer Reviews</Description>
# <URL>
# https://www.amazon.de/review/product/B01E9I4D0G?SubscriptionId=AKIAI5WV5JKODYZMIJJA&tag=amazon00f-20&linkCode=xm2&camp=2025&creative=12738&creativeASIN=B01E9I4D0G
# </URL>
# </ItemLink>
# <ItemLink>
# <Description>All Offers</Description>
# <URL>
# https://www.amazon.de/gp/offer-listing/B01E9I4D0G?SubscriptionId=AKIAI5WV5JKODYZMIJJA&tag=amazon00f-20&linkCode=xm2&camp=2025&creative=12738&creativeASIN=B01E9I4D0G
# </URL>
# </ItemLink>
# </ItemLinks>
# <SmallImage>
# <URL>
# https://images-eu.ssl-images-amazon.com/images/I/51Ks29kTpLL._SL75_.jpg
# </URL>
# <Height Units="pixels">75</Height>
# <Width Units="pixels">74</Width>
# </SmallImage>
# <MediumImage>
# <URL>
# https://images-eu.ssl-images-amazon.com/images/I/51Ks29kTpLL._SL160_.jpg
# </URL>
# <Height Units="pixels">160</Height>
# <Width Units="pixels">158</Width>
# </MediumImage>
# <LargeImage>
# <URL>
# https://images-eu.ssl-images-amazon.com/images/I/51Ks29kTpLL.jpg
# </URL>
# <Height Units="pixels">500</Height>
# <Width Units="pixels">495</Width>
# </LargeImage>
# <ImageSets>
# <MerchantId>A13Y3NQPC8V2N9</MerchantId>
# <ImageSet Category="primary">
# <SwatchImage>
# <URL>
# https://images-eu.ssl-images-amazon.com/images/I/51Ks29kTpLL._SL30_.jpg
# </URL>
# <Height Units="pixels">30</Height>
# <Width Units="pixels">30</Width>
# </SwatchImage>
# <SmallImage>
# <URL>
# https://images-eu.ssl-images-amazon.com/images/I/51Ks29kTpLL._SL75_.jpg
# </URL>
# <Height Units="pixels">75</Height>
# <Width Units="pixels">74</Width>
# </SmallImage>
# <ThumbnailImage>
# <URL>
# https://images-eu.ssl-images-amazon.com/images/I/51Ks29kTpLL._SL75_.jpg
# </URL>
# <Height Units="pixels">75</Height>
# <Width Units="pixels">74</Width>
# </ThumbnailImage>
# <TinyImage>
# <URL>
# https://images-eu.ssl-images-amazon.com/images/I/51Ks29kTpLL._SL110_.jpg
# </URL>
# <Height Units="pixels">110</Height>
# <Width Units="pixels">109</Width>
# </TinyImage>
# <MediumImage>
# <URL>
# https://images-eu.ssl-images-amazon.com/images/I/51Ks29kTpLL._SL160_.jpg
# </URL>
# <Height Units="pixels">160</Height>
# <Width Units="pixels">158</Width>
# </MediumImage>
# <LargeImage>
# <URL>
# https://images-eu.ssl-images-amazon.com/images/I/51Ks29kTpLL.jpg
# </URL>
# <Height Units="pixels">500</Height>
# <Width Units="pixels">495</Width>
# </LargeImage>
# </ImageSet>
# </ImageSets>
# <ItemAttributes>
# <BatteriesIncluded>0</BatteriesIncluded>
# <Binding>Elektronik</Binding>
# <Brand>Printing Pleasure</Brand>
# <CatalogNumberList>
# <CatalogNumberListElement>others=a_H540-3A/320-3A/210-3A-1set</CatalogNumberListElement>
# <CatalogNumberListElement>PPa_hp540-3A/320-3A/210-3A-1set</CatalogNumberListElement>
# </CatalogNumberList>
# <Color>4er Set</Color>
# <CompatibleDevices>
# HP Colour Laserjet CM1312, CM1312n, CM1312nf, CM1312nfi, CM1312nfi MFP, CP1210, CP1213, CP1214, CP1514n, CP1215, CP1215n, CP1216, CP1217, CP1500, CP1510, CP1513, CP1514, CP1514n, CP1515, CP1515n, CP1516n, CP1517n, CP1518, CP1518n, CP1518ni, CP1519n, Canon I-Sensys LBP-8030, LBP-8050CN, LBP-8050, LBP-8030CN, LBP-5050, LBP-5050N, MF-8030CN, MF-8040CN, MF-8050CN, MF-8080CW, HP Colour Laserjet Pro CM1415, CM1415FN, CM1415FNW, CP1525, CP1525N, CP1525NW, HP LaserJet Pro 200 Color M251n, M251nw, MFP M276n, MFP M276nw, Canon LBP-7100CN, LBP-7110CW, MF-8230CN, MF-8280CW
# </CompatibleDevices>
# <EAN>0604007441691</EAN>
# <EANList>
# <EANListElement>0604007441691</EANListElement>
# </EANList>
# <Feature>
# Erstklassige Druckqualität, hohe Zuverlässigkeit und einfache Handhabung
# </Feature>
# <Feature>Maximaler Inhalt, 100% Kompatibel</Feature>
# <Feature>3 Jahre Garantie</Feature>
# <Feature>
# Geeignet für folgende Geräte: HP Colour Laserjet CM1312, CM1312n, CM1312nf, CM1312nfi, CM1312nfi MFP, CP1210, CP1213, CP1214, CP1514n, CP1215, CP1215n, CP1216, CP1217, CP1500, CP1510, CP1513, CP1514, CP1514n, CP1515, CP1515n, CP1516n, CP1517n, CP1518, CP1518n, CP1518ni, CP1519n, Canon I-Sensys LBP-8030, LBP-8050CN, LBP-8050, LBP-8030CN, LBP-5050, LBP-5050N, MF-8030CN, MF-8040CN, MF-8050CN, MF-8080CW
# </Feature>
# <Feature>
# Geeignet für folgende Geräte: HP Colour Laserjet Pro CM1415, CM1415FN, CM1415FNW, CP1525, CP1525N, CP1525NW, HP LaserJet Pro 200 Color M251n, M251nw, MFP M276n, MFP M276nw, Canon LBP-7100CN, LBP-7110CW, MF-8230CN, MF-8280CW
# </Feature>
# <GLProductGroup>Office Product</GLProductGroup>
# <Label>Printing Pleasure no HP original</Label>
# <ListPrice>
# <Amount>4290</Amount>
# <CurrencyCode>EUR</CurrencyCode>
# <FormattedPrice>EUR 42,90</FormattedPrice>
# </ListPrice>
# <Manufacturer>Printing Pleasure no HP original</Manufacturer>
# <MPN>a_hp540-3A/320-3A/210-3A-1SET</MPN>
# <NumberOfItems>4</NumberOfItems>
# <PackageDimensions>
# <Height Units="hundredths-inches">890</Height>
# <Length Units="hundredths-inches">1350</Length>
# <Weight Units="Hundertstel Pfund">608</Weight>
# <Width Units="hundredths-inches">900</Width>
# </PackageDimensions>
# <PartNumber>a_hp540-3A/320-3A/210-3A-1SET</PartNumber>
# <ProductGroup>Office Product</ProductGroup>
# <ProductSiteLaunchDate>2016-04-14</ProductSiteLaunchDate>
# <ProductTypeName>INK_OR_TONER</ProductTypeName>
# <Publisher>Printing Pleasure no HP original</Publisher>
# <Studio>Printing Pleasure no HP original</Studio>
# <Title>
# 4 Toner kompatibel für HP Laserjet Pro 200 Color M251n M251nw MFP M276n MFP M276nw CM1312 CM1312n CM1312nf CM1312nfi CM1312nfi MFP CP1210 CP1213 CP1214 CP1514n CP1215 CP1215n CP1216 CP1217 CP1500 CP1510 CP1513 CP1514 CP1514n CP1515 CP1515n CP1516n CP1517n CP1518 CP1518n CP1518ni CP1519n CM1415 CM1415FN CM1415FNW CP1525 CP1525N CP1525NW - Schwarz/Cyan/Magenta/Gelb, hohe Kapazität
# </Title>
# <UPC>604007441691</UPC>
# <UPCList>
# <UPCListElement>604007441691</UPCListElement>
# </UPCList>
# </ItemAttributes>
# </Item>
# </Items>
# </ItemLookupResponse>"""
# titleList=[]
# titleRegex = '<Title>([\w\W]*?)<\/Title>'
# titleList.append(ftfy.fix_text(html.unescape(str(re.findall(titleRegex, str(itemloop))[0].lower()))))
# print(titleList)
#
#

from gensim.models import Word2Vec

#loading the downloaded model
model = Word2Vec.load_word2vec_format('GoogleNews-vectors-negative300.bin', binary=True, norm_only=True)

#the model is loaded. It can be used to perform all of the tasks mentioned above.

# getting word vectors of a word
dog = model['dog']

#performing king queen magic
print(model.most_similar(positive=['woman', 'king'], negative=['man']))

#picking odd one out
print(model.doesnt_match("breakfast cereal dinner lunch".split()))

#printing similarity index
print(model.similarity('woman', 'man'))