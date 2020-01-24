/*
 * Copyright 2014-2019 Andrew Gaul <andrew@gaul.org>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gaul.s3proxy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.MultipartPart;
import org.jclouds.blobstore.domain.MultipartUpload;
import org.jclouds.blobstore.options.CopyOptions;
import org.jclouds.blobstore.options.CreateContainerOptions;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.blobstore.util.ForwardingBlobStore;
import org.jclouds.domain.Location;
import org.jclouds.io.Payload;

import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;

/** This class is a BlobStore wrapper which sends a HTTP requests on blob creations. */
final class WebhookBlobStore extends ForwardingBlobStore {
    private final String webhook;

    private WebhookBlobStore(BlobStore blobStore, String webhook) {
        super(blobStore);
        this.webhook = webhook;
    }

    static BlobStore newWebhookBlobStore(BlobStore blobStore, String webhook) {
        return new WebhookBlobStore(blobStore, webhook);
    }

    private void sendEvent(String containerName, String name) {
        try {
            CloseableHttpClient httpClient = HttpClients.createDefault();
            try {
                List<NameValuePair> urlParameters = new ArrayList<>();
                urlParameters.add(new BasicNameValuePair("bucket", containerName));
                urlParameters.add(new BasicNameValuePair("key", name));
                HttpPost post = new HttpPost(this.webhook);
                post.setEntity(new UrlEncodedFormEntity(urlParameters));
                httpClient.execute(post);
            } finally {
                httpClient.close();
            }
        } catch (Exception e) {
            System.out.println(e);
            e.printStackTrace();
        }

    }

    @Override
    public String putBlob(String containerName, Blob blob) {
        String result = super.putBlob(containerName, blob);
        this.sendEvent(containerName, blob.getMetadata().getName());
        return result;
    }

    @Override
    public String putBlob(final String containerName, Blob blob,
            final PutOptions options) {
        String result = super.putBlob(containerName, blob, options);
        this.sendEvent(containerName, blob.getMetadata().getName());
        return result;
    }

    @Override
    public String copyBlob(final String fromContainer, final String fromName,
            final String toContainer, final String toName,
            final CopyOptions options) {
        String result = super.copyBlob(fromContainer, fromName, toContainer, toName, options);
        this.sendEvent(toContainer, toName);
        return result;
    }

    @Override
    public String completeMultipartUpload(final MultipartUpload mpu,
            final List<MultipartPart> parts) {
        String result = super.completeMultipartUpload(mpu, parts);
        this.sendEvent(mpu.containerName(), mpu.blobName());
        return result;
    }
}
