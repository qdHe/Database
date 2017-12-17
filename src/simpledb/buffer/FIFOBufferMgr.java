package simpledb.buffer;

import java.util.HashMap;
import java.util.logging.Level;

import simpledb.file.Block;
import simpledb.server.SimpleDB;

/**
 * CS 4432 Project 1
 *
 * We created this class to handle the FIFO replacement policy for a buffer
 * manager.
 *
 * This class handles pinning and unpinning buffers in memory using the least
 * recently used policy for buffer replacement.
 *
 * @author Lambert Wang
 */
public class FIFOBufferMgr extends AbstractBufferMgr {

	// The map of the memory buffers
	protected HashMap<Block, FIFOBuffer> buffer;

	/**
	 * Creates a FIFOBufferMgr instance with the specified maximum number of
	 * buffers.
	 *
	 * @param numbuffs
	 *            The maximum number of buffers for memory.
	 */
	public FIFOBufferMgr(int numbuffs) {
		super(numbuffs);
		buffer = new HashMap<Block, FIFOBuffer>();
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see simpledb.buffer.AbstractBufferMgr#available()
	 */
	@Override
	public int available() {
		return numAvailable;
	}

	/**
	 * Chooses an unpinned buffer to replace with a new page. If there is no
	 * space in the memory buffer, the least recently used buffer is emptied and
	 * returned.
	 *
	 * (non-Javadoc)
	 *
	 * @see simpledb.buffer.AbstractBufferMgr#chooseUnpinnedBuffer()
	 */
	@Override
	protected Buffer chooseUnpinnedBuffer() {
		printBufferContents();
		long startTime = System.nanoTime();
		long endTime;
		if (numAvailable > 0) {
			endTime = System.nanoTime();
			SimpleDB.getLogger().log(Level.INFO, "Time elapsed: " + (endTime - startTime) + " ns");
			return new FIFOBuffer();
		}


		Buffer ret = findFirstIn();

		endTime = System.nanoTime();
		SimpleDB.getLogger().log(Level.INFO, "Time elapsed: " + (endTime - startTime) + " ns");

		return ret;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * simpledb.buffer.AbstractBufferMgr#findExistingBuffer(simpledb.file.Block)
	 */
	@Override
	protected Buffer findExistingBuffer(Block blk) {
		long startTime = System.nanoTime();

		//这里不需要重新设置时间
		FIFOBuffer buff = buffer.get(blk);
		//if (buff != null) {
		//	buff.setJoinTimeMillis();
		//}

		long endTime = System.nanoTime();
		SimpleDB.getLogger().log(Level.INFO, "Time elapsed: " + (endTime - startTime) + " ns");

		return buff;
	}

	/**
	 * Finds the least recently used buffer and removes it from memory. If the
	 * memory buffer is empty, returns a new buffer to write into. Otherwise,
	 * returns the least recently used buffer.
	 */
	protected synchronized FIFOBuffer findFirstIn() {
		long time = -1;
		Block blk = null;

		// Check if buffer is empty
		if (buffer.keySet().size() <= 0) {
			return new FIFOBuffer();
		}

		// Compare each least recently used time with the smallest value to find
		// the time farthest in the part.
		for (Block block : buffer.keySet()) {
			FIFOBuffer buff = buffer.get(block);
			if (!buff.isPinned()) {
				if (time == -1) {
					time = buff.getJoinTimeMillis();
					blk = block;
				}
				else if (time > buff.getJoinTimeMillis()) {
					time = buff.getJoinTimeMillis();
					blk = block;
				}
			}
		}

		if (blk != null) {
			SimpleDB.getLogger().log(Level.INFO, "Removed block: " + blk + " from buffer");
			return buffer.remove(blk);
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see simpledb.buffer.AbstractBufferMgr#flushAll(int)
	 */
	@Override
	protected synchronized void flushAll(int txnum) {
		for (Block block : buffer.keySet()) {
			if (buffer.get(block).isModifiedBy(txnum)) {
				buffer.get(block).flush();
			}
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see simpledb.buffer.AbstractBufferMgr#pin(simpledb.file.Block)
	 */
	@Override
	protected synchronized Buffer pin(Block blk) {
		Buffer buff = findExistingBuffer(blk);
		SimpleDB.getLogger().log(Level.INFO, "Searched for existing block: " + blk + " and block was: " + buff);

		if (buff == null) {
			buff = chooseUnpinnedBuffer();
			if (buff == null) {
				SimpleDB.getLogger().log(Level.SEVERE, "Unpinned buffer was null");
				return null;
			}
			buff.assignToBlock(blk);
			buffer.put(blk, (FIFOBuffer) buff);

			if (!buff.isPinned()) {
				numAvailable--;
				numAvailable = (numAvailable < 0) ? 0 : numAvailable;
			}
		}

		SimpleDB.getLogger().log(Level.INFO, "Number available: " + numAvailable);
		buff.pin();

		//这个地方的设置其实是不需要的？
		((FIFOBuffer) buff).setJoinTimeMillis();

		return buff;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see simpledb.buffer.AbstractBufferMgr#pinNew(java.lang.String,
	 * simpledb.buffer.PageFormatter)
	 */
	@Override
	protected synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
		Buffer buff = chooseUnpinnedBuffer();
		if (buff == null) {
			SimpleDB.getLogger().log(Level.SEVERE, "Unpinned buffer was null");
			return null;
		}

		buff.assignToNew(filename, fmtr);
		SimpleDB.getLogger().log(Level.INFO, "Pinned new block: " + buff.block());
		buffer.put(buff.block(), (FIFOBuffer) buff);

		numAvailable--;
		numAvailable = (numAvailable < 0) ? 0 : numAvailable;
		SimpleDB.getLogger().log(Level.INFO, "Number available: " + numAvailable);

		buff.pin();

		((FIFOBuffer) buff).setJoinTimeMillis();

		SimpleDB.getLogger().log(Level.INFO, "New block: " + buff);
		return buff;
	}

	/**
	 * Prints the buffer contents to the log output.
	 */
	protected void printBufferContents() {
		String output = "";
		for (Block blk : buffer.keySet()) {
			output += blk + ": " + buffer.get(blk) + "\n";
		}

		SimpleDB.getLogger().log(Level.INFO, "\n\nBuffer Contents:\n" + output);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see simpledb.buffer.AbstractBufferMgr#unpin(simpledb.buffer.Buffer)
	 */
	@Override
	protected synchronized void unpin(Buffer buff) {
		buff.unpin();
		//这个地方的set到底还需要吗？
		((FIFOBuffer) buff).setJoinTimeMillis();
		SimpleDB.getLogger().log(Level.INFO, "Buffer unpinned: " + buff);
	}
}
