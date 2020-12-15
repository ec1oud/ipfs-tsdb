/****************************************************************************
**
** Copyright (C) 2020 Shawn Rutledge
**
** This file is free software; you can redistribute it and/or
** modify it under the terms of the GNU General Public License
** version 3 as published by the Free Software Foundation
** and appearing in the file LICENSE included in the packaging
** of this file.
**
** This code is distributed in the hope that it will be useful,
** but WITHOUT ANY WARRANTY; without even the implied warranty of
** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
** GNU General Public License for more details.
**
****************************************************************************/

#include <QCoreApplication>
#include <QCborArray>
#include <QCborMap>
#include <QCborValue>
#include <QDebug>
#include <QFile>
#include <QJsonDocument>
#include <QJsonObject>

static const QLatin1String TypeKey("type");
static const QLatin1String ValuesKey("values");

/*!
    A utility to create a "head record" of the form
    {
      "_timestamp": {
        "type": "u64",
        "values": b""
      },
      "numeric_field": {
        "type": "f32"
        "values": b""
      }
    }

    where b"" represents a byte array, NOT a string.
    (This can be represented in CBOR but not in JSON.)
    It can be inserted as a DAG node like this:
    $ ipfs dag put --input-enc cbor headRecord.cbor
    However, updating it is difficult, because of
    https://github.com/ipfs/go-ipfs/issues/4313 :
    the IPFS HTTP API doesn't provide a way to read it back
    in CBOR format, only as JSON, but it can't be correctly
    represented in JSON.
*/
int main(int argc, char *argv[])
{
    if (argc < 2)
        qFatal("required argument: file.json");

    QCoreApplication a(argc, argv);

    QFile j(a.arguments().last());
    if (!j.open(QIODevice::ReadOnly)) {
        qFatal("couldn't open input file");
    }

    QJsonDocument jd = QJsonDocument::fromJson(j.readAll());
    QCborMap headRecord;

    Q_ASSERT(jd.isObject());
    auto jdo = jd.object();
    auto it = jdo.constBegin();
    while (it != jdo.constEnd()) {
        Q_ASSERT(it.value().isObject());
        auto kv = it.value().toObject();
        qDebug() << it.key() << kv;
        Q_ASSERT(kv.contains(TypeKey));
        QCborMap field;
        field.insert(TypeKey, kv.value(TypeKey).toString());
        field.insert(ValuesKey, QByteArray());
//        field.insert(ValuesKey, QByteArray::fromHex("9f018202039f0405ffff"));
        headRecord.insert(it.key(), field);
        ++it;
    }

    qDebug() << headRecord;

    QFile f("headRecord.cbor");
    if (!f.open(QIODevice::WriteOnly)) {
        qWarning("Couldn't write file.");
        return -1;
    }

    f.write(headRecord.toCborValue().toCbor());
    f.close();
}
