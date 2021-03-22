FROM python:3.7.10-alpine3.13
RUN apk --update add git \
    && rm -rf /var/lib/apt/lists/* \
    && rm /var/cache/apk/* \
    && cd / \
    && git clone -b wampy https://github.com/CheerL/goodmorning.git \
    && cd goodmorning \
    && pip3 install ./requirements.txt \
    && chmod +x ./run.sh
WORKDIR /goodmorning
VOLUME [ "/goodmorning/log", '/goodmorning/config' ]
ENTRYPOINT [ "./run.sh" ]
CMD [ "1" ]