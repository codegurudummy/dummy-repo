package concurrency;

import com.amazon.matrix.model.Revision;
import com.amazon.matrix.ops.*;
import lombok.RequiredArgsConstructor;
import test.amazon.keyedblobmatrix.testframework.assertingmatrix.Resettable;

import java.util.Arrays;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

/**
 * This is a class that replays predefined sequence of requests to generate requests.
 * If it has already generated the last provided script, it would reuse the last request
 * for all future requests of the same type until another request is registered.
 * If a type of requests was never provided and is requested, it will throw an exception.
 * @author chenshao
 *
 */
@RequiredArgsConstructor
public class StandardMatrixRequestGenerator implements MatrixRequestGenerator, Resettable
{
    private final Queue<GetMatrixRequest> getMatrixRequests = new ConcurrentLinkedQueue<>();
    private final Queue<GetRevisionRequest> getRevisionRequests = new ConcurrentLinkedQueue<>();
    private final Queue<PostRevisionRequest> postRevisionRequests = new ConcurrentLinkedQueue<>();
    private final Queue<PutMatrixRequest> putMatrixRequests = new ConcurrentLinkedQueue<>();
    private final Queue<DeleteMatrixRequest> deleteMatrixRequests = new ConcurrentLinkedQueue<>();
    private final Queue<DiffRevisionRequest> diffRevisionRequests = new ConcurrentLinkedQueue<>();
    
    public StandardMatrixRequestGenerator withGetMatrixRequest(GetMatrixRequest... req)
    {
        getMatrixRequests.addAll(Arrays.stream(req).collect(Collectors.toList()));
        return this;
    }
    
    public StandardMatrixRequestGenerator withGetRevisionRequest(GetRevisionRequest... req)
    {
        getRevisionRequests.addAll(Arrays.stream(req).collect(Collectors.toList()));
        return this;
    }    
    public StandardMatrixRequestGenerator withPutMatrixRequest(PutMatrixRequest... req)
    {
        putMatrixRequests.addAll(Arrays.stream(req).collect(Collectors.toList()));
        return this;
    }    
    public StandardMatrixRequestGenerator withPostRevisionRequest(PostRevisionRequest... req)
    {
        postRevisionRequests.addAll(Arrays.stream(req).collect(Collectors.toList()));
        return this;
    }    
    public StandardMatrixRequestGenerator withDiffRevisionRequest(DiffRevisionRequest... req)
    {
        diffRevisionRequests.addAll(Arrays.stream(req).collect(Collectors.toList()));
        return this;
    }    
    public StandardMatrixRequestGenerator withDeleteMatrixRequest(DeleteMatrixRequest... req)
    {
        deleteMatrixRequests.addAll(Arrays.stream(req).collect(Collectors.toList()));
        return this;
    }
    
    @Override
    public GetMatrixRequest getNextGetMatrixRequest(String matrixId)
    {
        GetMatrixRequest req = getMatrixRequests.peek();
        if (Objects.isNull(req)) throw new RuntimeException("No GetMatrixRequest provided during construction.");
        if (getMatrixRequests.size()==1) return req;
        return getMatrixRequests.poll();
    }

    @Override
    public GetRevisionRequest getNextGetRevisionRequest(String matrixId, String basisRevisionId)
    {
        GetRevisionRequest req = getRevisionRequests.peek();
        if (Objects.isNull(req)) throw new RuntimeException("No GetRevisionRequest provided during construction.");
        if (getRevisionRequests.size()==1) return req;
        return getRevisionRequests.poll();
    }

    @Override
    public PostRevisionRequest getNextPostRevisionRequest(String matrixId, String basisRevisionId, Revision revisionToPost)
    {
        PostRevisionRequest req = postRevisionRequests.peek();
        if (Objects.isNull(req)) throw new RuntimeException("No PostRevisionRequest provided during construction.");
        if (postRevisionRequests.size()==1) return req;
        return postRevisionRequests.poll();
    }

    @Override
    public PutMatrixRequest getNextPutMatrixRequest(String matrixId, String basisRevisionId, String newRevisionId)
    {
        PutMatrixRequest req = putMatrixRequests.peek();
        if (Objects.isNull(req)) throw new RuntimeException("No PutMatrixRequest provided during construction.");
        if (putMatrixRequests.size()==1) return req;
        return putMatrixRequests.poll();
    }

    @Override
    public DeleteMatrixRequest getNextDeleteRequest(String matrixId, String revisionId)
    {
        DeleteMatrixRequest req = deleteMatrixRequests.peek();
        if (Objects.isNull(req)) throw new RuntimeException("No DeleteMatrixRequest provided during construction.");
        if (deleteMatrixRequests.size()==1) return req;
        return deleteMatrixRequests.poll();
    }

    @Override
    public DiffRevisionRequest getNextDiffRevisionRequest(String matrixId, String fromRevisionId, String toRevisionId)
    {
        DiffRevisionRequest req = diffRevisionRequests.peek();
        if (Objects.isNull(req)) throw new RuntimeException("No DiffRevisionRequest provided during construction.");
        if (diffRevisionRequests.size()==1) return req;
        return diffRevisionRequests.poll();
    }

    @Override
    public void reset()
    {
        // do nothing
    }
}