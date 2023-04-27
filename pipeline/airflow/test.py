
star_wars_comment = {
                        'comment_id': '384yt9fqhu',
                        'video_title': 'Cool Beans',
                    }

star_wars_comment2 = {
                        'comment_id': 'udfgiug3qirt',
                        'video_title': 'Awesome Sauce',
                    }

star_wars_comment3 = {
                        'comment_id': 'udfgiug3qirt',
                        'video_title': 'Boom Tube',
                    }

star_wars_comment4 = {
                        'comment_id': '438798',
                        'video_title': 'Boom Tube',
                    }

lst = []
lst.append(star_wars_comment)
lst.append(star_wars_comment4)

print(f'\nBEFORE: {lst}')

more = []

if star_wars_comment3['comment_id'] not in [comment['comment_id'] for comment in lst]:
    lst.append(star_wars_comment3)
    
print(f'\nAFTER: {lst}')