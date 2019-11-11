import os

import boto3
import spacy


def nlp_with_spacy(event, context):
    execute(event)


def write_to_file(output, file_name):
    with open('{}'.format("/tmp/" + file_name),
              'a') as output_file:
        output_file.write(output + "\n")


dep_output = []


def showTree(sent):
    def __showTree(token, level):
        tab = "\t" * level
        dep_output.append("\n{}[".format(tab))
        [__showTree(t, level + 1) for t in token.lefts]
        dep_output.append("\n{}\t{} [{}] ({})".format(tab, token, token.dep_, token.tag_))
        [__showTree(t, level + 1) for t in token.rights]
        dep_output.append("\n{}]".format(tab))

    return __showTree(sent.root, 1)


def filtered_chunks(doc, pattern):
    for chunk in doc.noun_chunks:
        signature = ''.join(['<%s>' % w.tag_ for w in chunk])
        if pattern.match(signature) is not None:
            yield chunk


def chunking(doc, key):
    write_to_file("<<SNP>>", key)
    for chunk in doc.noun_chunks:
        write_to_file(chunk.text, key)

    write_to_file("<<ENP>>", key)


def execute(event):
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    print('Bucket name is : {}'.format(bucket_name))

    s3 = boto3.resource('s3')
    key = event['Records'][0]['s3']['object']['key']
    print('Key is : {}'.format(key))
    obj = s3.Object(bucket_name, key)
    body = obj.get()['Body'].read()
    if os.path.exists("/tmp/" + key):
        os.remove('/tmp/' + key)
    write_to_file('<<BEGIN>>', key)
    print('processing file')
    process_file(str(body, 'utf-8'), key)
    print('file processed')
    write_to_file('<<END>>', key)
    print('sending file to s3')
    send_file_to_s3(key)
    print('file sent to s3')


def send_file_to_s3(file_name):
    s3 = boto3.client('s3')
    s3.upload_file("/tmp/" + file_name, 'ph-spacy-output', '{}'.format(file_name))


def process_file(data, key):
    nlp = spacy.load('en_core_web_sm')
    tokens = nlp(data)

    data_map = {}
    for token in tokens:
        if data_map.get(token.sent) is None:
            internal_list = []
            internal_list.append((token.text, token.tag_))
            data_map[token.sent] = internal_list
        else:
            data_map[token.sent].append((token.text, token.tag_))
    for sentence in data_map:
        write_to_file(str(sentence), key)
        write_to_file(str(data_map[sentence]), key)
    chunking(tokens, key)
    write_to_file('<<SDEP>>', key)
    [showTree(token.sent) for token in tokens if (showTree(token.sent) is not None)]
    for s in dep_output:
        write_to_file(s, key)

    write_to_file('<<EDEP>>', key)


if __name__ == '__main__':
    execute()
