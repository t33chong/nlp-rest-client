import sys, gensim, os

topics_to_counts, topics_to_maxes, topics_to_mins = dict(), dict(), dict()

for line in open(sys.argv[1], 'r').readlines():
    chops = line[:-1].split(',')
    wid = chops[0]
    for grouping in chops[1:]:
            (id, amt) = grouping.split('-')
            topics_to_counts[id] = topics_to_counts.get(id, 0) + 1
            if amt > topics_to_maxes.get(id, (0, 0))[0]:
                    topics_to_maxes[id] = (amt, wid)
            if amt < topics_to_mins.get(id, (1, 0))[0]:
                    topics_to_mins[id] = (amt, wid)

print "Top topics:"
print sorted(topics_to_counts.items(), key=lambda x: x[1], reverse=True)[:100]

model = gensim.models.LdaModel.load(os.getcwd()+'/'+'lda-5000wikis-999topics.model')
results = model.show_topics(topics=999, topn=10, formatted=True)
print map(lambda x: (x[1], results[int(x[0])]), sorted(topics_to_counts.items(), key=lambda x: x[1], reverse=True)[:20])
