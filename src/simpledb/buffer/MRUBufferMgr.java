package simpledb.buffer;

import java.util.HashMap;
import java.util.logging.Level;

import simpledb.file.Block;
import simpledb.server.SimpleDB;

/**
 * CS 4432 Project 1
 *
 * We created this class to handle the MRU replacement policy for a buffer
 * manager.
 *
 * This class handles pinning and unpinning buffers in memory using the least
 * recently used policy for buffer replacement.
 *
 * @author Lambert Wang
 */
public class MRUBufferMgr extends AbstractBufferMgr {

	// The map of the memory buffers
	protected HashMap<Block, MRUBuffer> buffer;

	/**
	 * Creates a MRUBufferMgr instance with the specified maximum number of
	 * buffers.
	 *
	 * @param numbuffs
	 *            The maximum number of buffers for memory.
	 */
	public MRUBufferMgr(int numbuffs) {
		super(numbuffs);
		buffer = new HashMap<Block, MRUBuffer>();
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
			return new MRUBuffer();
		}


		Buffer ret = findMostRecentlyUsed();

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

		MRUBuffer buff = buffer.get(blk);
		if (buff != null) {
			buff.setLeastRecentlyUsedTimeMillis();
		}

		long endTime = System.nanoTime();
		SimpleDB.getLogger().log(Level.INFO, "Time elapsed: " + (endTime - startTime) + " ns");

		return buff;
	}

	/**
	 * Finds the least recently used buffer and removes it from memory. If the
	 * memory buffer is empty, returns a new buffer to write into. Otherwise,
	 * returns the least recently used buffer.
	 */
	protected synchronized MRUBuffer findMostRecentlyUsed() {
		long time = -1;
		Block blk = null;

		// Check if buffer is empty
		if (buffer.keySet().size() <= 0) {
			return new MRUBuffer();
		}

		// Compare each most recently used time with the smallest value to find
		// the time farthest in the part.
		for (Block block : buffer.keySet()) {
			MRUBuffer buff = buffer.get(block);
			if (!buff.isPinned()) {
				if (time == -1) {
					time = buff.getLeastRecentlyUsedTimeMillis();
					blk = block;
				} else if (time <= buff.getLeastRecentlyUsedTimeMillis()) {
					time = buff.getLeastRecentlyUsedTimeMillis();
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
			buffer.put(blk, (MRUBuffer) buff);

			if (!buff.isPinned()) {
				numAvailable--;
				numAvailable = (numAvailable < 0) ? 0 : numAvailable;
			}
		}

		SimpleDB.getLogger().log(Level.INFO, "Number available: " + numAvailable);
		buff.pin();

		((MRUBuffer) buff).setLeastRecentlyUsedTimeMillis();

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
		buffer.put(buff.block(), (MRUBuffer) buff);

		numAvailable--;
		numAvailable = (numAvailable < 0) ? 0 : numAvailable;
		SimpleDB.getLogger().log(Level.INFO, "Number available: " + numAvailable);

		buff.pin();

		((MRUBuffer) buff).setLeastRecentlyUsedTimeMillis();

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
		((MRUBuffer) buff).setLeastRecentlyUsedTimeMillis();
		SimpleDB.getLogger().log(Level.INFO, "Buffer unpinned: " + buff);
	}
}
